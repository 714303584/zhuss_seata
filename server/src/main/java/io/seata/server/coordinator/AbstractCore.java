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

import java.io.IOException;
import java.util.concurrent.TimeoutException;

import io.seata.core.context.RootContext;
import io.seata.core.exception.BranchTransactionException;
import io.seata.core.exception.GlobalTransactionException;
import io.seata.core.exception.TransactionException;
import io.seata.core.exception.TransactionExceptionCode;
import io.seata.core.model.BranchStatus;
import io.seata.core.model.BranchType;
import io.seata.core.model.GlobalStatus;
import io.seata.core.protocol.transaction.BranchCommitRequest;
import io.seata.core.protocol.transaction.BranchCommitResponse;
import io.seata.core.protocol.transaction.BranchRollbackRequest;
import io.seata.core.protocol.transaction.BranchRollbackResponse;
import io.seata.core.rpc.RemotingServer;
import io.seata.server.lock.LockManager;
import io.seata.server.lock.LockerManagerFactory;
import io.seata.server.session.BranchSession;
import io.seata.server.session.GlobalSession;
import io.seata.server.session.SessionHelper;
import io.seata.server.session.SessionHolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import static io.seata.core.exception.TransactionExceptionCode.BranchTransactionNotExist;
import static io.seata.core.exception.TransactionExceptionCode.FailedToAddBranch;
import static io.seata.core.exception.TransactionExceptionCode.GlobalTransactionNotActive;
import static io.seata.core.exception.TransactionExceptionCode.GlobalTransactionStatusInvalid;
import static io.seata.core.exception.TransactionExceptionCode.FailedToSendBranchCommitRequest;
import static io.seata.core.exception.TransactionExceptionCode.FailedToSendBranchRollbackRequest;

/**
 * The type abstract core.
 *  分支类型的核心
 * @author ph3636
 */
public abstract class AbstractCore implements Core {

    protected static final Logger LOGGER = LoggerFactory.getLogger(AbstractCore.class);

    //获取锁管理者
    protected LockManager lockManager = LockerManagerFactory.getLockManager();

    protected RemotingServer remotingServer;

    public AbstractCore(RemotingServer remotingServer) {
        if (remotingServer == null) {
            throw new IllegalArgumentException("remotingServer must be not null");
        }
        this.remotingServer = remotingServer;
    }

    public abstract BranchType getHandleBranchType();

    /**
     * 进行分支事务的注册
     * @param branchType the branch type
     * @param resourceId the resource id
     * @param clientId   the client id
     * @param xid        the xid
     * @param applicationData the context
     * @param lockKeys   the lock keys
     * @return
     * @throws TransactionException
     */
    @Override
    public Long branchRegister(BranchType branchType, String resourceId, String clientId, String xid,
                               String applicationData, String lockKeys) throws TransactionException {
        GlobalSession globalSession = assertGlobalSessionNotNull(xid, false);
        return SessionHolder.lockAndExecute(globalSession, () -> {
            globalSessionStatusCheck(globalSession);
            globalSession.addSessionLifecycleListener(SessionHolder.getRootSessionManager());
            BranchSession branchSession = SessionHelper.newBranchByGlobal(globalSession, branchType, resourceId,
                    applicationData, lockKeys, clientId);
            MDC.put(RootContext.MDC_KEY_BRANCH_ID, String.valueOf(branchSession.getBranchId()));
            branchSessionLock(globalSession, branchSession);
            try {
                globalSession.addBranch(branchSession);
            } catch (RuntimeException ex) {
                branchSessionUnlock(branchSession);
                throw new BranchTransactionException(FailedToAddBranch, String
                        .format("Failed to store branch xid = %s branchId = %s", globalSession.getXid(),
                                branchSession.getBranchId()), ex);
            }
            if (LOGGER.isInfoEnabled()) {
                LOGGER.info("Register branch successfully, xid = {}, branchId = {}, resourceId = {} ,lockKeys = {}",
                    globalSession.getXid(), branchSession.getBranchId(), resourceId, lockKeys);
            }
            return branchSession.getBranchId();
        });
    }

    protected void globalSessionStatusCheck(GlobalSession globalSession) throws GlobalTransactionException {
        if (!globalSession.isActive()) {
            throw new GlobalTransactionException(GlobalTransactionNotActive, String.format(
                "Could not register branch into global session xid = %s status = %s, cause by globalSession not active",
                globalSession.getXid(), globalSession.getStatus()));
        }
        if (globalSession.getStatus() != GlobalStatus.Begin) {
            throw new GlobalTransactionException(GlobalTransactionStatusInvalid, String
                    .format("Could not register branch into global session xid = %s status = %s while expecting %s",
                            globalSession.getXid(), globalSession.getStatus(), GlobalStatus.Begin));
        }
    }

    protected void branchSessionLock(GlobalSession globalSession, BranchSession branchSession) throws TransactionException {

    }

    protected void branchSessionUnlock(BranchSession branchSession) throws TransactionException {

    }

    private GlobalSession assertGlobalSessionNotNull(String xid, boolean withBranchSessions)
            throws TransactionException {
        GlobalSession globalSession = SessionHolder.findGlobalSession(xid, withBranchSessions);
        if (globalSession == null) {
            throw new GlobalTransactionException(TransactionExceptionCode.GlobalTransactionNotExist,
                    String.format("Could not found global transaction xid = %s, may be has finished.", xid));
        }
        return globalSession;
    }

    @Override
    public void branchReport(BranchType branchType, String xid, long branchId, BranchStatus status,
                             String applicationData) throws TransactionException {
        GlobalSession globalSession = assertGlobalSessionNotNull(xid, true);
        BranchSession branchSession = globalSession.getBranch(branchId);
        if (branchSession == null) {
            throw new BranchTransactionException(BranchTransactionNotExist,
                    String.format("Could not found branch session xid = %s branchId = %s", xid, branchId));
        }
        globalSession.addSessionLifecycleListener(SessionHolder.getRootSessionManager());
        globalSession.changeBranchStatus(branchSession, status);

        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("Report branch status successfully, xid = {}, branchId = {}", globalSession.getXid(),
                branchSession.getBranchId());
        }
    }

    @Override
    public boolean lockQuery(BranchType branchType, String resourceId, String xid, String lockKeys)
            throws TransactionException {
        return true;
    }

    @Override
    public BranchStatus branchCommit(GlobalSession globalSession, BranchSession branchSession) throws TransactionException {
        try {
            BranchCommitRequest request = new BranchCommitRequest();
            request.setXid(branchSession.getXid());
            request.setBranchId(branchSession.getBranchId());
            request.setResourceId(branchSession.getResourceId());
            request.setApplicationData(branchSession.getApplicationData());
            request.setBranchType(branchSession.getBranchType());
            return branchCommitSend(request, globalSession, branchSession);
        } catch (IOException | TimeoutException e) {
            throw new BranchTransactionException(FailedToSendBranchCommitRequest,
                    String.format("Send branch commit failed, xid = %s branchId = %s", branchSession.getXid(),
                            branchSession.getBranchId()), e);
        }
    }

    protected BranchStatus branchCommitSend(BranchCommitRequest request, GlobalSession globalSession,
                                            BranchSession branchSession) throws IOException, TimeoutException {
        BranchCommitResponse response = (BranchCommitResponse) remotingServer.sendSyncRequest(
                branchSession.getResourceId(), branchSession.getClientId(), request);
        return response.getBranchStatus();
    }

    /**
     * 进行回滚
     * @param globalSession the global session 全局事务会话
     * @param branchSession the branch session 分支事务会话
     * @return
     * @throws TransactionException
     */
    @Override
    public BranchStatus branchRollback(GlobalSession globalSession, BranchSession branchSession) throws TransactionException {
        try {
            //构建事务回滚请求
            //事务回滚请求
            BranchRollbackRequest request = new BranchRollbackRequest();
            request.setXid(branchSession.getXid());
            request.setBranchId(branchSession.getBranchId());
            request.setResourceId(branchSession.getResourceId());
            request.setApplicationData(branchSession.getApplicationData());
            request.setBranchType(branchSession.getBranchType());
            return branchRollbackSend(request, globalSession, branchSession);
        } catch (IOException | TimeoutException e) {
            throw new BranchTransactionException(FailedToSendBranchRollbackRequest,
                    String.format("Send branch rollback failed, xid = %s branchId = %s",
                            branchSession.getXid(), branchSession.getBranchId()), e);
        }
    }

    /**
     * 向指定TC发送回滚请求
     * @param request
     * @param globalSession
     * @param branchSession
     * @return
     * @throws IOException
     * @throws TimeoutException
     */
    protected BranchStatus branchRollbackSend(BranchRollbackRequest request, GlobalSession globalSession,
                                              BranchSession branchSession) throws IOException, TimeoutException {


        BranchRollbackResponse response = (BranchRollbackResponse) remotingServer.sendSyncRequest(
                branchSession.getResourceId(), branchSession.getClientId(), request);
        return response.getBranchStatus();
    }

    /**
     * 开启全局事务
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
        return null;
    }

    /**
     * 进行全局事务提交
     * @param xid XID of the global transaction.
     * @return
     * @throws TransactionException
     */
    @Override
    public GlobalStatus commit(String xid) throws TransactionException {
        return null;
    }

    /**
     * 全局事务提交
     * @param globalSession the global session
     * @param retrying      the retrying
     *                          是否进行重试中
     * @return
     * @throws TransactionException
     */
    @Override
    public boolean doGlobalCommit(GlobalSession globalSession, boolean retrying) throws TransactionException {
        return true;
    }

    /**
     * 全局事务上报
     * @param xid XID of the global transaction.
     *              全局事务的XID
     * @param globalStatus Status of the global transaction.
     *                     全局事务的状态
     * @return
     * @throws TransactionException
     */
    @Override
    public GlobalStatus globalReport(String xid, GlobalStatus globalStatus) throws TransactionException {
        return null;
    }

    /**
     * 全局事务回滚
     * @param xid XID of the global transaction
     * @return
     * @throws TransactionException
     */
    @Override
    public GlobalStatus rollback(String xid) throws TransactionException {
        return null;
    }

    /**
     * 全局事务回滚
     * @param globalSession the global session
     *                          全局事务会话
     * @param retrying      the retrying
     * @return
     * @throws TransactionException
     */
    @Override
    public boolean doGlobalRollback(GlobalSession globalSession, boolean retrying) throws TransactionException {
        return true;
    }

    /**
     * 获取事务状态
     * @param xid XID of the global transaction.
     * @return
     * @throws TransactionException
     */
    @Override
    public GlobalStatus getStatus(String xid) throws TransactionException {
        return null;
    }

    /**
     * 全局事务上报
     * @param globalSession the global session
     *                      全局事务会话
     * @param xid           Transaction id.
     *                      事务ID
     * @param globalStatus
     * @throws TransactionException
     */
    @Override
    public void doGlobalReport(GlobalSession globalSession, String xid, GlobalStatus globalStatus) throws TransactionException {

    }
}
