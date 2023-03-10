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
package io.seata.rm.datasource.exec;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.StringJoiner;

import io.seata.common.util.IOUtil;
import io.seata.common.util.StringUtils;
import io.seata.config.Configuration;
import io.seata.config.ConfigurationFactory;
import io.seata.core.constants.ConfigurationKeys;
import io.seata.common.DefaultValues;
import io.seata.rm.datasource.ColumnUtils;
import io.seata.rm.datasource.SqlGenerateUtils;
import io.seata.rm.datasource.StatementProxy;
import io.seata.rm.datasource.sql.struct.TableMeta;
import io.seata.rm.datasource.sql.struct.TableRecords;
import io.seata.sqlparser.ParametersHolder;
import io.seata.sqlparser.SQLRecognizer;
import io.seata.sqlparser.SQLUpdateRecognizer;

/**
 * The type Update executor.
 * 更新sql的执行器
 * @param <T> the type parameter
 * @param <S> the type parameter
 * @author sharajava
 */
public class UpdateExecutor<T, S extends Statement> extends AbstractDMLBaseExecutor<T, S> {

    private static final Configuration CONFIG = ConfigurationFactory.getInstance();

    private static final boolean ONLY_CARE_UPDATE_COLUMNS = CONFIG.getBoolean(
            ConfigurationKeys.TRANSACTION_UNDO_ONLY_CARE_UPDATE_COLUMNS, DefaultValues.DEFAULT_ONLY_CARE_UPDATE_COLUMNS);

    /**
     * Instantiates a new Update executor.
     * 实例化一个更新执行者
     *
     * @param statementProxy    the statement proxy
     * @param statementCallback the statement callback
     * @param sqlRecognizer     the sql recognizer
     */
    public UpdateExecutor(StatementProxy<S> statementProxy, StatementCallback<T, S> statementCallback,
                          SQLRecognizer sqlRecognizer) {
        super(statementProxy, statementCallback, sqlRecognizer);
    }

    /**
     * 获取sql执行前的镜像
     * @return
     * @throws SQLException
     */
    @Override
    protected TableRecords beforeImage() throws SQLException {
        //sql参数
        ArrayList<List<Object>> paramAppenderList = new ArrayList<>();
        //获取表的元数据
        TableMeta tmeta = getTableMeta();
        //拼装查询语句
        String selectSQL = buildBeforeImageSQL(tmeta, paramAppenderList);
        //获取查询语句并获取查询到的记录
        return buildTableRecords(tmeta, selectSQL, paramAppenderList);
    }

    /**
     * 拼装更新SQL前的sql查询语句
     * @param tableMeta 表元数据
     * @param paramAppenderList 参数
     * @return
     */
    private String buildBeforeImageSQL(TableMeta tableMeta, ArrayList<List<Object>> paramAppenderList) {
        //sql更新语句解析器
        SQLUpdateRecognizer recognizer = (SQLUpdateRecognizer) sqlRecognizer;
        //获取更新的列
        List<String> updateColumns = recognizer.getUpdateColumns();
        //前置 select语句
        StringBuilder prefix = new StringBuilder("SELECT ");
        // 后置语句
        StringBuilder suffix = new StringBuilder(" FROM ").append(getFromTableInSQL());
        //获取where条件
        String whereCondition = buildWhereCondition(recognizer, paramAppenderList);
        if (StringUtils.isNotBlank(whereCondition)) {
            suffix.append(WHERE).append(whereCondition);
        }
        //获取orderBy
        String orderBy = recognizer.getOrderBy();
        if (StringUtils.isNotBlank(orderBy)) {
            suffix.append(orderBy);
        }
        //参数
        ParametersHolder parametersHolder = statementProxy instanceof ParametersHolder ? (ParametersHolder)statementProxy : null;
        //获取limit语句
        String limit = recognizer.getLimit(parametersHolder, paramAppenderList);
        if (StringUtils.isNotBlank(limit)) {
            suffix.append(limit);
        }
        //拼装for update 语句锁定要更新的数据
        suffix.append(" FOR UPDATE");
        //拼装select
        StringJoiner selectSQLJoin = new StringJoiner(", ", prefix.toString(), suffix.toString());
        if (ONLY_CARE_UPDATE_COLUMNS) {
            if (!containsPK(updateColumns)) {
                selectSQLJoin.add(getColumnNamesInSQL(tableMeta.getEscapePkNameList(getDbType())));
            }
            for (String columnName : updateColumns) {
                selectSQLJoin.add(columnName);
            }
        } else {
            for (String columnName : tableMeta.getAllColumns().keySet()) {
                selectSQLJoin.add(ColumnUtils.addEscape(columnName, getDbType()));
            }
        }
        //返回sql语句
        return selectSQLJoin.toString();
    }

    /**
     * 获取更新后的镜像 -- 基本
     * @param beforeImage the before image
     * @return
     * @throws SQLException
     */
    @Override
    protected TableRecords afterImage(TableRecords beforeImage) throws SQLException {
        TableMeta tmeta = getTableMeta();
        if (beforeImage == null || beforeImage.size() == 0) {
            return TableRecords.empty(getTableMeta());
        }
        //获取更新后的查询语句
        String selectSQL = buildAfterImageSQL(tmeta, beforeImage);
        ResultSet rs = null;
        try (PreparedStatement pst = statementProxy.getConnection().prepareStatement(selectSQL)) {
            SqlGenerateUtils.setParamForPk(beforeImage.pkRows(), getTableMeta().getPrimaryKeyOnlyName(), pst);
            rs = pst.executeQuery();
            return TableRecords.buildRecords(tmeta, rs);
        } finally {
            IOUtil.close(rs);
        }
    }

    /**
     * 拼装查询后的sql语句
     * @param tableMeta
     * @param beforeImage
     * @return
     * @throws SQLException
     */
    private String buildAfterImageSQL(TableMeta tableMeta, TableRecords beforeImage) throws SQLException {
        StringBuilder prefix = new StringBuilder("SELECT ");
        String whereSql = SqlGenerateUtils.buildWhereConditionByPKs(tableMeta.getPrimaryKeyOnlyName(), beforeImage.pkRows().size(), getDbType());
        String suffix = " FROM " + getFromTableInSQL() + " WHERE " + whereSql;
        StringJoiner selectSQLJoiner = new StringJoiner(", ", prefix.toString(), suffix);
        if (ONLY_CARE_UPDATE_COLUMNS) {
            SQLUpdateRecognizer recognizer = (SQLUpdateRecognizer) sqlRecognizer;
            List<String> updateColumns = recognizer.getUpdateColumns();
            if (!containsPK(updateColumns)) {
                selectSQLJoiner.add(getColumnNamesInSQL(tableMeta.getEscapePkNameList(getDbType())));
            }
            for (String columnName : updateColumns) {
                selectSQLJoiner.add(columnName);
            }
        } else {
            for (String columnName : tableMeta.getAllColumns().keySet()) {
                selectSQLJoiner.add(ColumnUtils.addEscape(columnName, getDbType()));
            }
        }
        return selectSQLJoiner.toString();
    }

}
