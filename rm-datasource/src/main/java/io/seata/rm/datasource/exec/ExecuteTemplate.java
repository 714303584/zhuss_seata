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

import io.seata.common.loader.EnhancedServiceLoader;
import io.seata.common.util.CollectionUtils;
import io.seata.core.context.RootContext;
import io.seata.core.model.BranchType;
import io.seata.rm.datasource.StatementProxy;
import io.seata.rm.datasource.sql.SQLVisitorFactory;
import io.seata.sqlparser.SQLRecognizer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

/**
 * The type Execute template.
 *  seata的执行模版
 * @author sharajava
 */
public class ExecuteTemplate {
    static Logger logger = LoggerFactory.getLogger(ExecuteTemplate.class);

    /**
     * Execute t.
     *
     * @param <T>               the type parameter
     * @param <S>               the type parameter
     * @param statementProxy    the statement proxy
     * @param statementCallback the statement callback
     * @param args              the args
     * @return the t
     * @throws SQLException the sql exception
     */
    public static <T, S extends Statement> T execute(StatementProxy<S> statementProxy,
                                                     StatementCallback<T, S> statementCallback,
                                                     Object... args) throws SQLException {
        return execute(null, statementProxy, statementCallback, args);
    }

    /**
     * Execute t.
     *  执行模板的执行代码
     * @param <T>               the type parameter 参数
     * @param <S>               the type parameter
     * @param sqlRecognizers    the sql recognizer list
     * @param statementProxy    the statement proxy
     * @param statementCallback the statement callback
     * @param args              the args
     * @return the t
     * @throws SQLException the sql exception
     */
    public static <T, S extends Statement> T execute(List<SQLRecognizer> sqlRecognizers,
                                                     StatementProxy<S> statementProxy,
                                                     StatementCallback<T, S> statementCallback,
                                                     Object... args) throws SQLException {
        //全局所 并且不等于AT事物模式 直接返回
        if (!RootContext.requireGlobalLock() && BranchType.AT != RootContext.getBranchType()) {
            // Just work as original statement
            //仅以原始statement运行
            return statementCallback.execute(statementProxy.getTargetStatement(), args);
        }
        //获取数据库类型
        String dbType = statementProxy.getConnectionProxy().getDbType();
        logger.info("ifreeshare -- ExecuteTemplate.execute.dbType:"+dbType);
        if (CollectionUtils.isEmpty(sqlRecognizers)) {
            //获取sql识别器
            sqlRecognizers = SQLVisitorFactory.get(
                    statementProxy.getTargetSQL(),
                    dbType);
        }
        Executor<T> executor;
        //执行器
        if (CollectionUtils.isEmpty(sqlRecognizers)) {
            executor = new PlainExecutor<>(statementProxy, statementCallback);
        } else {
            //获取到sql执行器
            if (sqlRecognizers.size() == 1) {
                SQLRecognizer sqlRecognizer = sqlRecognizers.get(0);
                //获取sql类型
                switch (sqlRecognizer.getSQLType()) {
                    case INSERT:
                        //根据执行刑期获取相应的插入执行器
                        executor = EnhancedServiceLoader.load(InsertExecutor.class, dbType,
                                new Class[]{StatementProxy.class, StatementCallback.class, SQLRecognizer.class},
                                new Object[]{statementProxy, statementCallback, sqlRecognizer});
                        break;
                    case UPDATE:
                        //更新执行器
                        executor = new UpdateExecutor<>(statementProxy, statementCallback, sqlRecognizer);
                        break;
                    case DELETE:
                        //删除执行器
                        executor = new DeleteExecutor<>(statementProxy, statementCallback, sqlRecognizer);
                        break;
                    case SELECT_FOR_UPDATE:
                        //查询执行器
                        executor = new SelectForUpdateExecutor<>(statementProxy, statementCallback, sqlRecognizer);
                        break;
                    default:
                        executor = new PlainExecutor<>(statementProxy, statementCallback);
                        break;
                }
            } else {
                //执行器
                executor = new MultiExecutor<>(statementProxy, statementCallback, sqlRecognizers);
            }
        }
        T rs;
        try {
            //通过执行器进行sql执行
            rs = executor.execute(args);
        } catch (Throwable ex) {
            if (!(ex instanceof SQLException)) {
                // Turn other exception into SQLException
                ex = new SQLException(ex);
            }
            throw (SQLException) ex;
        }
        return rs;
    }

}
