/*
 *  Copyright 1999-2019 Seata.io Group.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package io.seata.rm.datasource.undo;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLIntegrityConstraintViolationException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import io.seata.common.Constants;
import io.seata.common.util.CollectionUtils;
import io.seata.common.util.SizeUtil;
import io.seata.config.ConfigurationFactory;
import io.seata.core.compressor.CompressorFactory;
import io.seata.core.compressor.CompressorType;
import io.seata.core.constants.ClientTableColumnsName;
import io.seata.core.constants.ConfigurationKeys;
import io.seata.core.exception.BranchTransactionException;
import io.seata.core.exception.TransactionException;
import io.seata.rm.datasource.ConnectionContext;
import io.seata.rm.datasource.ConnectionProxy;
import io.seata.rm.datasource.DataSourceProxy;
import io.seata.rm.datasource.sql.struct.TableMeta;
import io.seata.rm.datasource.sql.struct.TableMetaCacheFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static io.seata.common.DefaultValues.DEFAULT_TRANSACTION_UNDO_LOG_TABLE;
import static io.seata.common.DefaultValues.DEFAULT_CLIENT_UNDO_COMPRESS_ENABLE;
import static io.seata.common.DefaultValues.DEFAULT_CLIENT_UNDO_COMPRESS_TYPE;
import static io.seata.common.DefaultValues.DEFAULT_CLIENT_UNDO_COMPRESS_THRESHOLD;
import static io.seata.core.exception.TransactionExceptionCode.BranchRollbackFailed_Retriable;

/**
 * @author jsbxyyx
 */
public abstract class AbstractUndoLogManager implements UndoLogManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractUndoLogManager.class);

    /**
     * undolog的状态
     */
    protected enum State {
        /**
         * This state can be properly rolled back by services
         * 这个状态是可以进行事务回滚的
         *
         *
         */
        Normal(0),
        /**
         * This state prevents the branch transaction from inserting undo_log after the global transaction is rolled
         * back.
         * 这个状态防止分支事务在全局事务回滚后进行插入undolog
         */
        GlobalFinished(1);

        private int value;

        State(int value) {
            this.value = value;
        }

        public int getValue() {
            return value;
        }
    }

    protected static final String UNDO_LOG_TABLE_NAME = ConfigurationFactory.getInstance().getConfig(
        ConfigurationKeys.TRANSACTION_UNDO_LOG_TABLE, DEFAULT_TRANSACTION_UNDO_LOG_TABLE);

    protected static final String SELECT_UNDO_LOG_SQL = "SELECT * FROM " + UNDO_LOG_TABLE_NAME + " WHERE "
        + ClientTableColumnsName.UNDO_LOG_BRANCH_XID + " = ? AND " + ClientTableColumnsName.UNDO_LOG_XID
        + " = ? FOR UPDATE";

    protected static final String DELETE_UNDO_LOG_SQL = "DELETE FROM " + UNDO_LOG_TABLE_NAME + " WHERE "
        + ClientTableColumnsName.UNDO_LOG_BRANCH_XID + " = ? AND " + ClientTableColumnsName.UNDO_LOG_XID + " = ?";

    protected static final boolean ROLLBACK_INFO_COMPRESS_ENABLE = ConfigurationFactory.getInstance().getBoolean(
        ConfigurationKeys.CLIENT_UNDO_COMPRESS_ENABLE, DEFAULT_CLIENT_UNDO_COMPRESS_ENABLE);

    protected static final CompressorType ROLLBACK_INFO_COMPRESS_TYPE = CompressorType.getByName(ConfigurationFactory.getInstance().getConfig(
        ConfigurationKeys.CLIENT_UNDO_COMPRESS_TYPE, DEFAULT_CLIENT_UNDO_COMPRESS_TYPE));

    protected static final long ROLLBACK_INFO_COMPRESS_THRESHOLD = SizeUtil.size2Long(ConfigurationFactory.getInstance().getConfig(
            ConfigurationKeys.CLIENT_UNDO_COMPRESS_THRESHOLD, DEFAULT_CLIENT_UNDO_COMPRESS_THRESHOLD));

    private static final ThreadLocal<String> SERIALIZER_LOCAL = new ThreadLocal<>();

    public static String getCurrentSerializer() {
        return SERIALIZER_LOCAL.get();
    }

    public static void setCurrentSerializer(String serializer) {
        SERIALIZER_LOCAL.set(serializer);
    }

    public static void removeCurrentSerializer() {
        SERIALIZER_LOCAL.remove();
    }

    /**
     * Delete undo log.
     *
     * @param xid      the xid
     * @param branchId the branch id
     * @param conn     the conn
     * @throws SQLException the sql exception
     */
    @Override
    public void deleteUndoLog(String xid, long branchId, Connection conn) throws SQLException {
        LOGGER.info("删除UndoLog, XID:{}, branchId:{}, sql:{}",xid, branchId, DELETE_UNDO_LOG_SQL);
        try (PreparedStatement deletePST = conn.prepareStatement(DELETE_UNDO_LOG_SQL)) {
            deletePST.setLong(1, branchId);
            deletePST.setString(2, xid);
            deletePST.executeUpdate();
        } catch (Exception e) {
            if (!(e instanceof SQLException)) {
                e = new SQLException(e);
            }
            throw (SQLException) e;
        }
    }

    /**
     * batch Delete undo log.
     * 批量删除undoLog
     * @param xids xid
     * @param branchIds branch Id
     * @param conn connection
     */
    @Override
    public void batchDeleteUndoLog(Set<String> xids, Set<Long> branchIds, Connection conn) throws SQLException {

        LOGGER.info("批量删除UndoLog");
        if (CollectionUtils.isEmpty(xids) || CollectionUtils.isEmpty(branchIds)) {
            return;
        }
        int xidSize = xids.size();
        int branchIdSize = branchIds.size();
        String batchDeleteSql = toBatchDeleteUndoLogSql(xidSize, branchIdSize);
        try (PreparedStatement deletePST = conn.prepareStatement(batchDeleteSql)) {
            int paramsIndex = 1;
            for (Long branchId : branchIds) {
                deletePST.setLong(paramsIndex++, branchId);
            }
            for (String xid : xids) {
                deletePST.setString(paramsIndex++, xid);
            }
            int deleteRows = deletePST.executeUpdate();
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("batch delete undo log size {}", deleteRows);
            }
        } catch (Exception e) {
            if (!(e instanceof SQLException)) {
                e = new SQLException(e);
            }
            throw (SQLException) e;
        }
    }

    protected static String toBatchDeleteUndoLogSql(int xidSize, int branchIdSize) {
        StringBuilder sqlBuilder = new StringBuilder(64);
        sqlBuilder.append("DELETE FROM ").append(UNDO_LOG_TABLE_NAME).append(" WHERE  ").append(
            ClientTableColumnsName.UNDO_LOG_BRANCH_XID).append(" IN ");
        appendInParam(branchIdSize, sqlBuilder);
        sqlBuilder.append(" AND ").append(ClientTableColumnsName.UNDO_LOG_XID).append(" IN ");
        appendInParam(xidSize, sqlBuilder);

        LOGGER.info("批量删除SQL:{}", sqlBuilder.toString());
        return sqlBuilder.toString();
    }

    protected static void appendInParam(int size, StringBuilder sqlBuilder) {
        sqlBuilder.append(" (");
        for (int i = 0; i < size; i++) {
            sqlBuilder.append("?");
            if (i < (size - 1)) {
                sqlBuilder.append(",");
            }
        }
        sqlBuilder.append(") ");
    }

    /**
     * 判断undo是否可以操作
     * @param state
     * @return
     */
    protected static boolean canUndo(int state) {
        //undolog的状态是否正常
        return state == State.Normal.getValue();
    }

    protected String buildContext(String serializer, CompressorType compressorType) {
        Map<String, String> map = new HashMap<>();
        map.put(UndoLogConstants.SERIALIZER_KEY, serializer);
        map.put(UndoLogConstants.COMPRESSOR_TYPE_KEY, compressorType.name());
        return CollectionUtils.encodeMap(map);
    }

    /**
     * 将内容解析为Map集合
     * @param data
     * @return
     */
    protected Map<String, String> parseContext(String data) {
        return CollectionUtils.decodeMap(data);
    }

    /**
     * Flush undo logs.
     * 刷新Undolog
     * @param cp the cp
     * @throws SQLException the sql exception
     */
    @Override
    public void flushUndoLogs(ConnectionProxy cp) throws SQLException {
        ConnectionContext connectionContext = cp.getContext();
        if (!connectionContext.hasUndoLog()) {
            return;
        }
        //获取全局事务Xid
        String xid = connectionContext.getXid();
        //获取分支事务ID
        long branchId = connectionContext.getBranchId();

        //分支事务的undolog构建
        BranchUndoLog branchUndoLog = new BranchUndoLog();
        branchUndoLog.setXid(xid);
        branchUndoLog.setBranchId(branchId);
        //获取sqlUndolog
        branchUndoLog.setSqlUndoLogs(connectionContext.getUndoItems());
        //UndoLogpase
        UndoLogParser parser = UndoLogParserFactory.getInstance();
        byte[] undoLogContent = parser.encode(branchUndoLog);

        CompressorType compressorType = CompressorType.NONE;
        if (needCompress(undoLogContent)) {
            compressorType = ROLLBACK_INFO_COMPRESS_TYPE;
            undoLogContent = CompressorFactory.getCompressor(compressorType.getCode()).compress(undoLogContent);
        }

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Flushing UNDO LOG: {}", new String(undoLogContent, Constants.DEFAULT_CHARSET));
        }
        //插入undoLog
        insertUndoLogWithNormal(xid, branchId, buildContext(parser.getName(), compressorType), undoLogContent, cp.getTargetConnection());
    }

    /**
     * Undo.
     * undo操作 -- 进行分支事务的回滚
     *
     * XA 模式是调用数据库的回滚操作
     * AT 模式是读取事务的UndoLog进行数据更改
     *         更新操作 -- 将beforeImage中的数据重新写入数据库
     *         删除操作 -- 将删除前的数据重新写入数据库
     *         插入操作 -- 将已经写入的数据进行删除
     * TCC 模式是调用cancel中指定的方法进行事务的回滚操作
     * saga TODO 本地消息表
     * @param dataSourceProxy the data source proxy 数据库资源
     * @param xid             the xid   全局事务ID
     * @param branchId        the branch id 分支事务ID
     * @throws TransactionException the transaction exception
     */
    @Override
    public void undo(DataSourceProxy dataSourceProxy, String xid, long branchId) throws TransactionException {

        LOGGER.info("ifreeshare -- 虚拟的Undolog管理者 -- 进行undol操作。 xid:{}, branchId:{}", xid, branchId);
        //数据库连接
        Connection conn = null;
        //结果集
        ResultSet rs = null;
        //sql执行器
        PreparedStatement selectPST = null;
        //原先的自动提交
        boolean originalAutoCommit = true;

        for (; ; ) {
            try {
                //获取原数据库连接 -- 未经代理连接
                conn = dataSourceProxy.getPlainConnection();

                // The entire undo process should run in a local transaction.
                //
                if (originalAutoCommit = conn.getAutoCommit()) {
                    conn.setAutoCommit(false);
                }

                // Find UNDO LOG
                //查找UNDOLog
                selectPST = conn.prepareStatement(SELECT_UNDO_LOG_SQL);
                selectPST.setLong(1, branchId);
                selectPST.setString(2, xid);
                rs = selectPST.executeQuery();

                boolean exists = false;
                while (rs.next()) {
                    exists = true;

                    // It is possible that the server repeatedly sends a rollback request to roll back
                    // the same branch transaction to multiple processes,
                    // ensuring that only the undo_log in the normal state is processed.
                    //获取UndoLog的状态
                    int state = rs.getInt(ClientTableColumnsName.UNDO_LOG_LOG_STATUS);
                    //是否可以执行UNDO
                    if (!canUndo(state)) {
                        if (LOGGER.isInfoEnabled()) {
                            LOGGER.info("xid {} branch {}, ignore {} undo_log", xid, branchId, state);
                        }
                        return;
                    }

                    //获取undolog的内容
                    //也就是获取undolog的beforeImage和afterImage
                    String contextString = rs.getString(ClientTableColumnsName.UNDO_LOG_CONTEXT);
                    //解析context的内容 -- 将context内容解析为Map集合
                    Map<String, String> context = parseContext(contextString);
                    //获取回滚信息
                    byte[] rollbackInfo = getRollbackInfo(rs);
                    //获取序列化key
                    String serializer = context == null ? null : context.get(UndoLogConstants.SERIALIZER_KEY);
                    LOGGER.info("解析UndoLog");
                    //获取Undolog的解析方式
                    UndoLogParser parser = serializer == null ? UndoLogParserFactory.getInstance()
                        : UndoLogParserFactory.getInstance(serializer);
//                    LOGGER.info();
                    //分支事务的Undolog实体
                    BranchUndoLog branchUndoLog = parser.decode(rollbackInfo);

                    try {
                        // put serializer name to local
                        setCurrentSerializer(parser.getName());
                        //获取Undolog的执行sql
                        List<SQLUndoLog> sqlUndoLogs = branchUndoLog.getSqlUndoLogs();
//                        LOGGER.info("SQLUndoLog");
                        //sql大于一条
                        if (sqlUndoLogs.size() > 1) {
                            //反转sql -- TODO 为什么反转sql
                            Collections.reverse(sqlUndoLogs);
                        }
                        //逐条执行undo操作
                        for (SQLUndoLog sqlUndoLog : sqlUndoLogs) {
                            //sqlundoLog
                            //获取数据库表元数据
                            TableMeta tableMeta = TableMetaCacheFactory.getTableMetaCache(dataSourceProxy.getDbType()).getTableMeta(
                                conn, sqlUndoLog.getTableName(), dataSourceProxy.getResourceId());
                            //设置表元数据到
                            sqlUndoLog.setTableMeta(tableMeta);
                            //进行sql回滚
                            AbstractUndoExecutor undoExecutor = UndoExecutorFactory.getUndoExecutor(
                                dataSourceProxy.getDbType(), sqlUndoLog);
                            undoExecutor.executeOn(conn);
                        }
                    } finally {
                        // remove serializer name
                        removeCurrentSerializer();
                    }
                }

                // If undo_log exists, it means that the branch transaction has completed the first phase,
                // 如果存在undo_log，意味者分支事务的第一阶段已经完成
                // we can directly roll back and clean the undo_log
                // Otherwise, it indicates that there is an exception in the branch transaction,
                // causing undo_log not to be written to the database.
                // For example, the business processing timeout, the global transaction is the initiator rolls back.
                // To ensure data consistency, we can insert an undo_log with GlobalFinished state
                // to prevent the local transaction of the first phase of other programs from being correctly submitted.
                // See https://github.com/seata/seata/issues/489

                if (exists) {
                    LOGGER.info("删除UndoLog， xid：{}, branchId:{}", xid, branchId);
                    deleteUndoLog(xid, branchId, conn);
                    conn.commit();
                    if (LOGGER.isInfoEnabled()) {
                        LOGGER.info("xid {} branch {}, undo_log deleted with {}", xid, branchId,
                            State.GlobalFinished.name());
                    }
                } else {
                    insertUndoLogWithGlobalFinished(xid, branchId, UndoLogParserFactory.getInstance(), conn);
                    conn.commit();
                    if (LOGGER.isInfoEnabled()) {
                        LOGGER.info("xid {} branch {}, undo_log added with {}", xid, branchId,
                            State.GlobalFinished.name());
                    }
                }

                return;
            } catch (SQLIntegrityConstraintViolationException e) {
                // Possible undo_log has been inserted into the database by other processes, retrying rollback undo_log
                if (LOGGER.isInfoEnabled()) {
                    LOGGER.info("xid {} branch {}, undo_log inserted, retry rollback", xid, branchId);
                }
            } catch (Throwable e) {
                if (conn != null) {
                    try {
                        conn.rollback();
                    } catch (SQLException rollbackEx) {
                        LOGGER.warn("Failed to close JDBC resource while undo ... ", rollbackEx);
                    }
                }
                throw new BranchTransactionException(BranchRollbackFailed_Retriable, String
                    .format("Branch session rollback failed and try again later xid = %s branchId = %s %s", xid,
                        branchId, e.getMessage()), e);

            } finally {
                try {
                    if (rs != null) {
                        rs.close();
                    }
                    if (selectPST != null) {
                        selectPST.close();
                    }
                    if (conn != null) {
                        if (originalAutoCommit) {
                            conn.setAutoCommit(true);
                        }
                        conn.close();
                    }
                } catch (SQLException closeEx) {
                    LOGGER.warn("Failed to close JDBC resource while undo ... ", closeEx);
                }
            }
        }
    }

    /**
     * insert uodo log when global finished
     *
     * @param xid           the xid
     * @param branchId      the branchId
     * @param undoLogParser the undoLogParse
     * @param conn          sql connection
     * @throws SQLException SQLException
     */
    protected abstract void insertUndoLogWithGlobalFinished(String xid, long branchId, UndoLogParser undoLogParser,
                                                            Connection conn) throws SQLException;

    /**
     * insert uodo log when normal
     *
     * @param xid            the xid
     * @param branchId       the branchId
     * @param rollbackCtx    the rollbackContext
     * @param undoLogContent the undoLogContent
     * @param conn           sql connection
     * @throws SQLException SQLException
     */
    protected abstract void insertUndoLogWithNormal(String xid, long branchId, String rollbackCtx, byte[] undoLogContent,
                                                    Connection conn) throws SQLException;

    /**
     * RollbackInfo to bytes
     *  将回滚信息解析为byte数组
     * @param rs
     * @return
     * @throws SQLException SQLException
     */
    protected byte[] getRollbackInfo(ResultSet rs) throws SQLException  {
        byte[] rollbackInfo = rs.getBytes(ClientTableColumnsName.UNDO_LOG_ROLLBACK_INFO);

        String rollbackInfoContext = rs.getString(ClientTableColumnsName.UNDO_LOG_CONTEXT);
        Map<String, String> context = CollectionUtils.decodeMap(rollbackInfoContext);
        CompressorType compressorType = CompressorType.getByName(context.getOrDefault(UndoLogConstants.COMPRESSOR_TYPE_KEY,
                CompressorType.NONE.name()));
        return CompressorFactory.getCompressor(compressorType.getCode()).decompress(rollbackInfo);
    }

    /**
     * if the undoLogContent is big enough to be compress
     * @param undoLogContent undoLogContent
     * @return boolean
     */
    protected boolean needCompress(byte[] undoLogContent) {
        return ROLLBACK_INFO_COMPRESS_ENABLE && undoLogContent.length > ROLLBACK_INFO_COMPRESS_THRESHOLD;
    }
}
