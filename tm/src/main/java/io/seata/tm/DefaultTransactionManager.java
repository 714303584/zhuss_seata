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
package io.seata.tm;

import io.seata.core.exception.TmTransactionException;
import io.seata.core.exception.TransactionException;
import io.seata.core.exception.TransactionExceptionCode;
import io.seata.core.model.GlobalStatus;
import io.seata.core.model.TransactionManager;
import io.seata.core.protocol.ResultCode;
import io.seata.core.protocol.transaction.AbstractTransactionRequest;
import io.seata.core.protocol.transaction.AbstractTransactionResponse;
import io.seata.core.protocol.transaction.GlobalBeginRequest;
import io.seata.core.protocol.transaction.GlobalBeginResponse;
import io.seata.core.protocol.transaction.GlobalCommitRequest;
import io.seata.core.protocol.transaction.GlobalCommitResponse;
import io.seata.core.protocol.transaction.GlobalReportRequest;
import io.seata.core.protocol.transaction.GlobalReportResponse;
import io.seata.core.protocol.transaction.GlobalRollbackRequest;
import io.seata.core.protocol.transaction.GlobalRollbackResponse;
import io.seata.core.protocol.transaction.GlobalStatusRequest;
import io.seata.core.protocol.transaction.GlobalStatusResponse;
import io.seata.core.rpc.netty.TmNettyRemotingClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeoutException;

/**
 * The type Default transaction manager.
 *
 * @author sharajava
 */
public class DefaultTransactionManager implements TransactionManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultTransactionManager.class);

    /**
     * ????????????????????????????????????
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
        LOGGER.info(" ??????????????????????????????????????????");
        LOGGER.info("DefaultTransactionManager.begin! applicationId:{},transactionServiceGrou:{}, name:{}, timeout:{}",
                applicationId, transactionServiceGroup, name, timeout);
        //????????????????????????
        GlobalBeginRequest request = new GlobalBeginRequest();
        request.setTransactionName(name);
        request.setTimeout(timeout);
        LOGGER.info("??????????????????(GlobalBeginRequest):{}",request.toString());
        LOGGER.info("????????????????????????");
        //????????????????????????
        GlobalBeginResponse response = (GlobalBeginResponse) syncCall(request);
        LOGGER.info("??????????????????(GlobalBeginResponse):{}",response.toString());
        //??????????????????
        if (response.getResultCode() == ResultCode.Failed) {
            throw new TmTransactionException(TransactionExceptionCode.BeginFailed, response.getMsg());
        }
        //????????????XID
        return response.getXid();
    }

    /**
     * ??????????????????
     * @param xid XID of the global transaction.
     * @return
     * @throws TransactionException
     */
    @Override
    public GlobalStatus commit(String xid) throws TransactionException {
        LOGGER.info("????????????????????????,xid:{}",xid);
        //??????????????????????????????
        GlobalCommitRequest globalCommit = new GlobalCommitRequest();
        globalCommit.setXid(xid);
        LOGGER.info("??????????????????(GlobalCommitRequest):{}", globalCommit.toString());
        //??????????????????
        GlobalCommitResponse response = (GlobalCommitResponse) syncCall(globalCommit);
        LOGGER.info("??????????????????(GlobalCommitResponse):{}", response.toString());
        return response.getGlobalStatus();
    }

    @Override
    public GlobalStatus rollback(String xid) throws TransactionException {
        GlobalRollbackRequest globalRollback = new GlobalRollbackRequest();
        globalRollback.setXid(xid);
        LOGGER.info("ifreeshare -- ??????????????????(GlobalRollbackRequest):{}", globalRollback.toString());
        GlobalRollbackResponse response = (GlobalRollbackResponse) syncCall(globalRollback);
        LOGGER.info("ifreeshare -- ??????????????????(GlobalRollbackResponse):{}", response.toString());
        return response.getGlobalStatus();
    }

    @Override
    public GlobalStatus getStatus(String xid) throws TransactionException {
        GlobalStatusRequest queryGlobalStatus = new GlobalStatusRequest();
        queryGlobalStatus.setXid(xid);
        LOGGER.info("ifreeshare -- ??????????????????(GlobalStatusRequest):{}", queryGlobalStatus.toString());
        GlobalStatusResponse response = (GlobalStatusResponse) syncCall(queryGlobalStatus);
        LOGGER.info("ifreeshare -- ??????????????????(GlobalStatusRequest):{}", response.toString());
        return response.getGlobalStatus();
    }

    @Override
    public GlobalStatus globalReport(String xid, GlobalStatus globalStatus) throws TransactionException {
        GlobalReportRequest globalReport = new GlobalReportRequest();
        globalReport.setXid(xid);
        globalReport.setGlobalStatus(globalStatus);
        LOGGER.info(" ??????????????????(GlobalReportRequest):{}",globalReport.toString());
        GlobalReportResponse response = (GlobalReportResponse) syncCall(globalReport);
        LOGGER.info("??????????????????(GlobalReportResponse):{}", response.toString());
        return response.getGlobalStatus();
    }

    /**
     * ?????????????????????????????????
     * @param request
     * @return
     * @throws TransactionException
     */
    private AbstractTransactionResponse syncCall(AbstractTransactionRequest request) throws TransactionException {
        try {
            LOGGER.info("???????????????request.class:{}",request.getTypeCode());
            LOGGER.info("????????????, ???????????? request:{}",
                    request.toString());
            //???????????????????????????
            return (AbstractTransactionResponse) TmNettyRemotingClient.getInstance().sendSyncRequest(request);
        } catch (TimeoutException toe) {
            throw new TmTransactionException(TransactionExceptionCode.IO, "RPC timeout", toe);
        }
    }
}
