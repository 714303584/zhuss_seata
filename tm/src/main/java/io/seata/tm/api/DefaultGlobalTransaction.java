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
package io.seata.tm.api;

import io.seata.config.ConfigurationFactory;
import io.seata.core.constants.ConfigurationKeys;
import io.seata.core.context.RootContext;
import io.seata.core.exception.TransactionException;
import io.seata.core.model.GlobalStatus;
import io.seata.core.model.TransactionManager;
import io.seata.tm.TransactionManagerHolder;
import io.seata.tm.api.transaction.SuspendedResourcesHolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static io.seata.common.DefaultValues.DEFAULT_TM_COMMIT_RETRY_COUNT;
import static io.seata.common.DefaultValues.DEFAULT_TM_ROLLBACK_RETRY_COUNT;

/**
 * The type Default global transaction.
 *
 * @author sharajava
 */
public class DefaultGlobalTransaction implements GlobalTransaction {

    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultGlobalTransaction.class);

    private static final int DEFAULT_GLOBAL_TX_TIMEOUT = 60000;

    private static final String DEFAULT_GLOBAL_TX_NAME = "default";

    private TransactionManager transactionManager;

    private String xid;

    private GlobalStatus status;

    private GlobalTransactionRole role;

    private static final int COMMIT_RETRY_COUNT = ConfigurationFactory.getInstance().getInt(
        ConfigurationKeys.CLIENT_TM_COMMIT_RETRY_COUNT, DEFAULT_TM_COMMIT_RETRY_COUNT);

    private static final int ROLLBACK_RETRY_COUNT = ConfigurationFactory.getInstance().getInt(
        ConfigurationKeys.CLIENT_TM_ROLLBACK_RETRY_COUNT, DEFAULT_TM_ROLLBACK_RETRY_COUNT);

    /**
     * Instantiates a new Default global transaction.
     */
    DefaultGlobalTransaction() {
        this(null, GlobalStatus.UnKnown, GlobalTransactionRole.Launcher);
    }

    /**
     * Instantiates a new Default global transaction.
     * 实例一个默认的全局事务
     * @param xid    the xid
     * @param status the status
     * @param role   the role
     */
    DefaultGlobalTransaction(String xid, GlobalStatus status, GlobalTransactionRole role) {
        LOGGER.info("ifreeshare -- create default global transaction xid:{},status:{},role:{}",
                xid, status,role);
        //
        this.transactionManager = TransactionManagerHolder.get();
        this.xid = xid;
        this.status = status;
        this.role = role;
    }

    @Override
    public void begin() throws TransactionException {
        LOGGER.info("ifreeshare -- begin global transaction!");
        begin(DEFAULT_GLOBAL_TX_TIMEOUT);
    }

    @Override
    public void begin(int timeout) throws TransactionException {
        begin(timeout, DEFAULT_GLOBAL_TX_NAME);
    }

    /**
     * TM开启事务
     * 开启全局事务
     * @param timeout Given timeout in MILLISECONDS.
     * @param name    Given name.
     * @throws TransactionException
     */
    @Override
    public void begin(int timeout, String name) throws TransactionException {
        LOGGER.info("ifreeshare -- DefaultGlobalTransaction.begin() global transaction! timeout:{}, name:{}",
                timeout, name);
        if (role != GlobalTransactionRole.Launcher) {
            assertXIDNotNull();
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Ignore Begin(): just involved in global transaction [{}]", xid);
            }
            return;
        }
        assertXIDNull();
        String currentXid = RootContext.getXID();
        LOGGER.info("ifreeshare -- begin global transaction! currentXid:{}", currentXid);
        if (currentXid != null) {
            throw new IllegalStateException("Global transaction already exists," +
                " can't begin a new global transaction, currentXid = " + currentXid);
        }
        //事务管理器开启事务  == 获取事务的Xid
        xid = transactionManager.begin(null, null, name, timeout);
        //设置事务状态并绑定全局事务ID
        status = GlobalStatus.Begin;
        RootContext.bind(xid);
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("Begin new global transaction [{}]", xid);
        }
        //全局事务开启成功
        LOGGER.info("ifreeshare -- DefaultGlobalTransaction.begin() global transaction! currentXid:{}", currentXid);
    }

    /**
     * 进行分支提交
     * @throws TransactionException
     */
    @Override
    public void commit() throws TransactionException {
        if (role == GlobalTransactionRole.Participant) {
            // Participant has no responsibility of committing
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Ignore Commit(): just involved in global transaction [{}]", xid);
            }
            return;
        }
        //判定Xid不为空
        assertXIDNotNull();
        int retry = COMMIT_RETRY_COUNT <= 0 ? DEFAULT_TM_COMMIT_RETRY_COUNT : COMMIT_RETRY_COUNT;
        try {
            while (retry > 0) {
                try {
                    //TM进行事务提交
                    status = transactionManager.commit(xid);
                    break;
                } catch (Throwable ex) {
                    LOGGER.error("Failed to report global commit [{}],Retry Countdown: {}, reason: {}", this.getXid(), retry, ex.getMessage());
                    retry--;
                    if (retry == 0) {
                        throw new TransactionException("Failed to report global commit", ex);
                    }
                }
            }
        } finally {
            if (xid.equals(RootContext.getXID())) {
                suspend();
            }
        }
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("[{}] commit status: {}", xid, status);
        }
    }

    @Override
    public void rollback() throws TransactionException {
        LOGGER.info("ifreeshare -- DefaultGlobalTransaction.rollback() 全局事务回滚方法执行!");
        if (role == GlobalTransactionRole.Participant) {
            // Participant has no responsibility of rollback
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Ignore Rollback(): just involved in global transaction [{}]", xid);
            }
            return;
        }
        assertXIDNotNull();

        int retry = ROLLBACK_RETRY_COUNT <= 0 ? DEFAULT_TM_ROLLBACK_RETRY_COUNT : ROLLBACK_RETRY_COUNT;
        try {
            while (retry > 0) {
                try {
                    LOGGER.info("ifreeshare -- DefaultGlobalTransaction.rollback() 全局事务回滚方法执行! xid:{}");
                    status = transactionManager.rollback(xid);
                    break;
                } catch (Throwable ex) {
                    LOGGER.error("Failed to report global rollback [{}],Retry Countdown: {}, reason: {}", this.getXid(), retry, ex.getMessage());
                    retry--;
                    if (retry == 0) {
                        throw new TransactionException("Failed to report global rollback", ex);
                    }
                }
            }
        } finally {
            if (xid.equals(RootContext.getXID())) {
                suspend();
            }
        }
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("[{}] rollback status: {}", xid, status);
        }
    }

    @Override
    public SuspendedResourcesHolder suspend() throws TransactionException {
        // In order to associate the following logs with XID, first get and then unbind.
        String xid = RootContext.getXID();
        if (xid != null) {
            if (LOGGER.isInfoEnabled()) {
                LOGGER.info("Suspending current transaction, xid = {}", xid);
            }
            RootContext.unbind();
            return new SuspendedResourcesHolder(xid);
        } else {
            return null;
        }
    }

    @Override
    public void resume(SuspendedResourcesHolder suspendedResourcesHolder) throws TransactionException {
        if (suspendedResourcesHolder == null) {
            return;
        }
        String xid = suspendedResourcesHolder.getXid();
        RootContext.bind(xid);
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Resumimg the transaction,xid = {}", xid);
        }
    }

    @Override
    public GlobalStatus getStatus() throws TransactionException {
        if (xid == null) {
            return GlobalStatus.UnKnown;
        }
        status = transactionManager.getStatus(xid);
        return status;
    }

    @Override
    public String getXid() {
        return xid;
    }

    @Override
    public void globalReport(GlobalStatus globalStatus) throws TransactionException {
        assertXIDNotNull();

        if (globalStatus == null) {
            throw new IllegalStateException();
        }

        status = transactionManager.globalReport(xid, globalStatus);
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("[{}] report status: {}", xid, status);
        }

        if (xid.equals(RootContext.getXID())) {
            suspend();
        }
    }

    @Override
    public GlobalStatus getLocalStatus() {
        return status;
    }

    @Override
    public GlobalTransactionRole getGlobalTransactionRole() {
        return role;
    }

    private void assertXIDNotNull() {
        if (xid == null) {
            throw new IllegalStateException();
        }
    }

    private void assertXIDNull() {
        if (xid != null) {
            throw new IllegalStateException();
        }
    }
}
