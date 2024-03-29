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
package io.seata.server.coordinator;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import io.seata.common.exception.NotSupportYetException;
import io.seata.common.loader.EnhancedServiceLoader;
import io.seata.common.util.CollectionUtils;
import io.seata.core.context.RootContext;
import io.seata.core.event.EventBus;
import io.seata.core.event.GlobalTransactionEvent;
import io.seata.core.exception.TransactionException;
import io.seata.core.logger.StackTraceLogger;
import io.seata.core.model.BranchStatus;
import io.seata.core.model.BranchType;
import io.seata.core.model.GlobalStatus;
import io.seata.core.rpc.RemotingServer;
import io.seata.server.event.EventBusManager;
import io.seata.server.session.BranchSession;
import io.seata.server.session.GlobalSession;
import io.seata.server.session.SessionHelper;
import io.seata.server.session.SessionHolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import static io.seata.server.session.BranchSessionHandler.CONTINUE;

/**
 * The type Default core.
 * 默认核心
 * @author sharajava
 */
public class DefaultCore implements Core {

    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultCore.class);

    private EventBus eventBus = EventBusManager.get();

    /**
     * 根据不同的分支
     */
    private static Map<BranchType, AbstractCore> coreMap = new ConcurrentHashMap<>();

    /**
     * get the Default core.
     * 获取默认的TC核心
     * @param remotingServer the remoting server
     */
    public DefaultCore(RemotingServer remotingServer) {
        //获取所有的分支类型的核心
        List<AbstractCore> allCore = EnhancedServiceLoader.loadAll(AbstractCore.class,
            new Class[]{RemotingServer.class}, new Object[]{remotingServer});
        //将分支核心根据处理的分支事务类型注册到核心集合里
        if (CollectionUtils.isNotEmpty(allCore)) {
            for (AbstractCore core : allCore) {
                coreMap.put(core.getHandleBranchType(), core);
            }
        }
    }

    /**
     * get core
     * 根据分支事务的类型获取其核心
     * @param branchType the branchType
     * @return the core
     */
    public AbstractCore getCore(BranchType branchType) {
        AbstractCore core = coreMap.get(branchType);
        if (core == null) {
            throw new NotSupportYetException("unsupported type:" + branchType.name());
        }
        return core;
    }

    /**
     * only for mock
     *
     * @param branchType the branchType
     * @param core       the core
     */
    public void mockCore(BranchType branchType, AbstractCore core) {
        coreMap.put(branchType, core);
    }

    /**
     * 分支事务注册
     * @param branchType the branch type 分支事务类型 AT XA TCC SAGA
     * @param resourceId the resource id    资源ID
     * @param clientId   the client id  客户端id
     * @param xid        the xid    全局事务ID
     * @param applicationData the context
     * @param lockKeys   the lock keys 锁标识
     * @return
     * @throws TransactionException
     */
    @Override
    public Long branchRegister(BranchType branchType, String resourceId, String clientId, String xid,
                               String applicationData, String lockKeys) throws TransactionException {
        //以事务类型获取其处理
        return getCore(branchType).branchRegister(branchType, resourceId, clientId, xid,
            applicationData, lockKeys);
    }

    /**
     * 分支事务上报
     * @param branchType      the branch type
     *                          分支事务类型
     * @param xid             the xid
     *                         全局事务ID
     * @param branchId        the branch id
     *                        分支事务ID
     * @param status          the status
     *                         状态
     * @param applicationData the application data
     *
     * @throws TransactionException
     */
    @Override
    public void branchReport(BranchType branchType, String xid, long branchId, BranchStatus status,
                             String applicationData) throws TransactionException {
        getCore(branchType).branchReport(branchType, xid, branchId, status, applicationData);
    }

    /**
     * 锁查询
     * @param branchType the branch type
     *                   分支事务类型
     * @param resourceId the resource id
     *                   资源ID
     * @param xid        the xid
     *                   全局事务ID
     * @param lockKeys   the lock keys
     *                   锁主键
     * @return
     * @throws TransactionException
     */
    @Override
    public boolean lockQuery(BranchType branchType, String resourceId, String xid, String lockKeys)
        throws TransactionException {
        //根据分支事务类型获取核心 -- 进行锁查询
        return getCore(branchType).lockQuery(branchType, resourceId, xid, lockKeys);
    }

    /**
     * 进行事务提交
     * @param globalSession the global session
     * @param branchSession the branch session
     * @return
     * @throws TransactionException
     */
    @Override
    public BranchStatus branchCommit(GlobalSession globalSession, BranchSession branchSession) throws TransactionException {
        //根据事务类型获取事务核心 -- 进行分支事务提交
        return getCore(branchSession.getBranchType()).branchCommit(globalSession, branchSession);
    }

    //进行事务回滚
    @Override
    public BranchStatus branchRollback(GlobalSession globalSession, BranchSession branchSession) throws TransactionException {
        //根据不同的分支事务类型进行其回滚操作
        return getCore(branchSession.getBranchType()).branchRollback(globalSession, branchSession);
    }

    /**
     * 开始全局事务
     * @param applicationId           ID of the application who begins this transaction.
     * @param transactionServiceGroup ID of the transaction service group.
     * @param name                    Give a name to the global transaction.
     * @param timeout                 Timeout of the global transaction.
     * @return
     * @throws TransactionException
     */
    @Override
    public String begin(String applicationId, String transactionServiceGroup, String name, int timeout)
        throws TransactionException {
        //全局事务会话获取
        GlobalSession session = GlobalSession.createGlobalSession(applicationId, transactionServiceGroup, name,
            timeout);
        MDC.put(RootContext.MDC_KEY_XID, session.getXid());
        //session添加会话生命周期监听
        session.addSessionLifecycleListener(SessionHolder.getRootSessionManager());

        session.begin();

        // transaction start event
        //发送事件
        eventBus.post(new GlobalTransactionEvent(session.getTransactionId(), GlobalTransactionEvent.ROLE_TC,
            session.getTransactionName(), applicationId, transactionServiceGroup, session.getBeginTime(), null, session.getStatus()));

        return session.getXid();
    }

    /**
     *
     * @param xid XID of the global transaction.
     *              进行全局事务的提交
     * @return
     * @throws TransactionException
     */
    @Override
    public GlobalStatus commit(String xid) throws TransactionException {
        //事务的session
        GlobalSession globalSession = SessionHolder.findGlobalSession(xid);
        if (globalSession == null) {
            return GlobalStatus.Finished;
        }
        globalSession.addSessionLifecycleListener(SessionHolder.getRootSessionManager());
        // just lock changeStatus
        boolean shouldCommit = SessionHolder.lockAndExecute(globalSession, () -> {
            // Highlight: Firstly, close the session, then no more branch can be registered.
            globalSession.closeAndClean();
            if (globalSession.getStatus() == GlobalStatus.Begin) {
                if (globalSession.canBeCommittedAsync()) {
                    globalSession.asyncCommit();
                    return false;
                } else {
                    globalSession.changeStatus(GlobalStatus.Committing);
                    return true;
                }
            }
            return false;
        });

        if (shouldCommit) {
            boolean success = doGlobalCommit(globalSession, false);
            //If successful and all remaining branches can be committed asynchronously, do async commit.
            if (success && globalSession.hasBranch() && globalSession.canBeCommittedAsync()) {
                globalSession.asyncCommit();
                return GlobalStatus.Committed;
            } else {
                return globalSession.getStatus();
            }
        } else {
            return globalSession.getStatus() == GlobalStatus.AsyncCommitting ? GlobalStatus.Committed : globalSession.getStatus();
        }
    }

    @Override
    public boolean doGlobalCommit(GlobalSession globalSession, boolean retrying) throws TransactionException {
        boolean success = true;
        // start committing event
        eventBus.post(new GlobalTransactionEvent(globalSession.getTransactionId(), GlobalTransactionEvent.ROLE_TC,
            globalSession.getTransactionName(), globalSession.getApplicationId(), globalSession.getTransactionServiceGroup(),
            globalSession.getBeginTime(), null, globalSession.getStatus()));

        if (globalSession.isSaga()) {
            success = getCore(BranchType.SAGA).doGlobalCommit(globalSession, retrying);
        } else {
            Boolean result = SessionHelper.forEach(globalSession.getSortedBranches(), branchSession -> {
                // if not retrying, skip the canBeCommittedAsync branches
                if (!retrying && branchSession.canBeCommittedAsync()) {
                    return CONTINUE;
                }

                BranchStatus currentStatus = branchSession.getStatus();
                if (currentStatus == BranchStatus.PhaseOne_Failed) {
                    globalSession.removeBranch(branchSession);
                    return CONTINUE;
                }
                try {
                    BranchStatus branchStatus = getCore(branchSession.getBranchType()).branchCommit(globalSession, branchSession);

                    switch (branchStatus) {
                        case PhaseTwo_Committed:
                            globalSession.removeBranch(branchSession);
                            return CONTINUE;
                        case PhaseTwo_CommitFailed_Unretryable:
                            if (globalSession.canBeCommittedAsync()) {
                                LOGGER.error(
                                    "Committing branch transaction[{}], status: PhaseTwo_CommitFailed_Unretryable, please check the business log.", branchSession.getBranchId());
                                return CONTINUE;
                            } else {
                                SessionHelper.endCommitFailed(globalSession);
                                LOGGER.error("Committing global transaction[{}] finally failed, caused by branch transaction[{}] commit failed.", globalSession.getXid(), branchSession.getBranchId());
                                return false;
                            }
                        default:
                            if (!retrying) {
                                globalSession.queueToRetryCommit();
                                return false;
                            }
                            if (globalSession.canBeCommittedAsync()) {
                                LOGGER.error("Committing branch transaction[{}], status:{} and will retry later",
                                    branchSession.getBranchId(), branchStatus);
                                return CONTINUE;
                            } else {
                                LOGGER.error(
                                    "Committing global transaction[{}] failed, caused by branch transaction[{}] commit failed, will retry later.", globalSession.getXid(), branchSession.getBranchId());
                                return false;
                            }
                    }
                } catch (Exception ex) {
                    StackTraceLogger.error(LOGGER, ex, "Committing branch transaction exception: {}",
                        new String[] {branchSession.toString()});
                    if (!retrying) {
                        globalSession.queueToRetryCommit();
                        throw new TransactionException(ex);
                    }
                }
                return CONTINUE;
            });
            // Return if the result is not null
            if (result != null) {
                return result;
            }
            //If has branch and not all remaining branches can be committed asynchronously,
            //do print log and return false
            if (globalSession.hasBranch() && !globalSession.canBeCommittedAsync()) {
                LOGGER.info("Committing global transaction is NOT done, xid = {}.", globalSession.getXid());
                return false;
            }
        }
        //If success and there is no branch, end the global transaction.
        if (success && globalSession.getBranchSessions().isEmpty()) {
            SessionHelper.endCommitted(globalSession);

            // committed event
            eventBus.post(new GlobalTransactionEvent(globalSession.getTransactionId(), GlobalTransactionEvent.ROLE_TC,
                globalSession.getTransactionName(), globalSession.getApplicationId(), globalSession.getTransactionServiceGroup(),
                globalSession.getBeginTime(), System.currentTimeMillis(), globalSession.getStatus()));

            LOGGER.info("Committing global transaction is successfully done, xid = {}.", globalSession.getXid());
        }
        return success;
    }

    @Override
    public GlobalStatus rollback(String xid) throws TransactionException {
        GlobalSession globalSession = SessionHolder.findGlobalSession(xid);
        if (globalSession == null) {
            return GlobalStatus.Finished;
        }
        globalSession.addSessionLifecycleListener(SessionHolder.getRootSessionManager());
        // just lock changeStatus
        boolean shouldRollBack = SessionHolder.lockAndExecute(globalSession, () -> {
            globalSession.close(); // Highlight: Firstly, close the session, then no more branch can be registered.
            if (globalSession.getStatus() == GlobalStatus.Begin) {
                globalSession.changeStatus(GlobalStatus.Rollbacking);
                return true;
            }
            return false;
        });
        if (!shouldRollBack) {
            return globalSession.getStatus();
        }

        doGlobalRollback(globalSession, false);
        return globalSession.getStatus();
    }

    /**
     * 核心回滚方法
     * @param globalSession the global session
     * @param retrying      the retrying
     * @return
     * @throws TransactionException
     */
    @Override
    public boolean doGlobalRollback(GlobalSession globalSession, boolean retrying) throws TransactionException {
        boolean success = true;
        // start rollback event
        //eventBus发送事件
        eventBus.post(new GlobalTransactionEvent(globalSession.getTransactionId(),
                GlobalTransactionEvent.ROLE_TC, globalSession.getTransactionName(),
                globalSession.getApplicationId(),
                globalSession.getTransactionServiceGroup(), globalSession.getBeginTime(),
                null, globalSession.getStatus()));

        //处理SAGA事务
        if (globalSession.isSaga()) {
            //调用SAGA核心进行回滚
            success = getCore(BranchType.SAGA).doGlobalRollback(globalSession, retrying);
        } else {
            Boolean result = SessionHelper.forEach(globalSession.getReverseSortedBranches(), branchSession -> {
                BranchStatus currentBranchStatus = branchSession.getStatus();
                //第二阶段失败
                if (currentBranchStatus == BranchStatus.PhaseOne_Failed) {
                    globalSession.removeBranch(branchSession);
                    return CONTINUE;
                }
                try {
                    //进行事务回滚
                    BranchStatus branchStatus = branchRollback(globalSession, branchSession);
                    switch (branchStatus) {
                        case PhaseTwo_Rollbacked:
                            //第二阶段已经回滚
                            globalSession.removeBranch(branchSession);
                            LOGGER.info("Rollback branch transaction successfully, xid = {} branchId = {}", globalSession.getXid(), branchSession.getBranchId());
                            return CONTINUE;
                        case PhaseTwo_RollbackFailed_Unretryable:
                            //回滚失败
                            SessionHelper.endRollbackFailed(globalSession);
                            LOGGER.info("Rollback branch transaction fail and stop retry, xid = {} branchId = {}", globalSession.getXid(), branchSession.getBranchId());
                            return false;
                        default:
                            //正在回滚的中
                            LOGGER.info("Rollback branch transaction fail and will retry, xid = {} branchId = {}", globalSession.getXid(), branchSession.getBranchId());
                            if (!retrying) {
                                globalSession.queueToRetryRollback();
                            }
                            return false;
                    }
                } catch (Exception ex) {
                    StackTraceLogger.error(LOGGER, ex,
                        "Rollback branch transaction exception, xid = {} branchId = {} exception = {}",
                        new String[] {globalSession.getXid(), String.valueOf(branchSession.getBranchId()), ex.getMessage()});
                    //进行回滚
                    if (!retrying) {
                        globalSession.queueToRetryRollback();
                    }
                    throw new TransactionException(ex);
                }
            });
            // Return if the result is not null
            if (result != null) {
                return result;
            }

            // In db mode, there is a problem of inconsistent data in multiple copies, resulting in new branch
            // transaction registration when rolling back.
            // 1. New branch transaction and rollback branch transaction have no data association
            // 2. New branch transaction has data association with rollback branch transaction
            // The second query can solve the first problem, and if it is the second problem, it may cause a rollback
            // failure due to data changes.
            GlobalSession globalSessionTwice = SessionHolder.findGlobalSession(globalSession.getXid());
            if (globalSessionTwice != null && globalSessionTwice.hasBranch()) {
                LOGGER.info("Rollbacking global transaction is NOT done, xid = {}.", globalSession.getXid());
                return false;
            }
        }
        if (success) {

            SessionHelper.endRollbacked(globalSession);

            // rollbacked event
            //进行回滚事件
            eventBus.post(new GlobalTransactionEvent(globalSession.getTransactionId(),
                    GlobalTransactionEvent.ROLE_TC, globalSession.getTransactionName(),
                    globalSession.getApplicationId(),
                    globalSession.getTransactionServiceGroup(),
                    globalSession.getBeginTime(), System.currentTimeMillis(),
                    globalSession.getStatus()));

            LOGGER.info("Rollback global transaction successfully, xid = {}.", globalSession.getXid());
        }
        return success;
    }

    @Override
    public GlobalStatus getStatus(String xid) throws TransactionException {
        GlobalSession globalSession = SessionHolder.findGlobalSession(xid, false);
        if (globalSession == null) {
            return GlobalStatus.Finished;
        } else {
            return globalSession.getStatus();
        }
    }

    /**
     * 默认的事务上报
     * @param xid XID of the global transaction.
     * @param globalStatus Status of the global transaction.
     * @return
     * @throws TransactionException
     */
    @Override
    public GlobalStatus globalReport(String xid, GlobalStatus globalStatus) throws TransactionException {
        GlobalSession globalSession = SessionHolder.findGlobalSession(xid);
        if (globalSession == null) {
            return globalStatus;
        }
        globalSession.addSessionLifecycleListener(SessionHolder.getRootSessionManager());
        doGlobalReport(globalSession, xid, globalStatus);
        return globalSession.getStatus();
    }

    /**
     * 全局事务上报
     * @param globalSession the global session
     * @param xid           Transaction id.
     * @param globalStatus
     * @throws TransactionException
     */
    @Override
    public void doGlobalReport(GlobalSession globalSession, String xid, GlobalStatus globalStatus) throws TransactionException {
        if (globalSession.isSaga()) {
            getCore(BranchType.SAGA).doGlobalReport(globalSession, xid, globalStatus);
        }
    }
}
