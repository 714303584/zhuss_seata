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

    @Override
    public String begin(String applicationId, String transactionServiceGroup, String name, int timeout)
        throws TransactionException {
        LOGGER.info(" 默认事务管理器，开启事务方法");
        LOGGER.info("DefaultTransactionManager.begin! applicationId:{},transactionServiceGrou:{}, name:{}, timeout:{}",
                applicationId, transactionServiceGroup, name, timeout);
        //全局开始请求
        GlobalBeginRequest request = new GlobalBeginRequest();
        request.setTransactionName(name);
        request.setTimeout(timeout);
        LOGGER.info("事务开启实体(GlobalBeginRequest):{}",request.toString());
        LOGGER.info("发送开启事务请求");
        GlobalBeginResponse response = (GlobalBeginResponse) syncCall(request);
        LOGGER.info("事务开启返回(GlobalBeginResponse):{}",response.toString());
        if (response.getResultCode() == ResultCode.Failed) {
            throw new TmTransactionException(TransactionExceptionCode.BeginFailed, response.getMsg());
        }
        return response.getXid();
    }

    @Override
    public GlobalStatus commit(String xid) throws TransactionException {
        LOGGER.info("默认的事务管理器,xid:{}",xid);
        GlobalCommitRequest globalCommit = new GlobalCommitRequest();
        globalCommit.setXid(xid);
        LOGGER.info("事务提交请求(GlobalCommitRequest):{}", globalCommit.toString());
        GlobalCommitResponse response = (GlobalCommitResponse) syncCall(globalCommit);
        LOGGER.info("事务提交请求(GlobalCommitResponse):{}", response.toString());
        return response.getGlobalStatus();
    }

    @Override
    public GlobalStatus rollback(String xid) throws TransactionException {
        GlobalRollbackRequest globalRollback = new GlobalRollbackRequest();
        globalRollback.setXid(xid);
        LOGGER.info("ifreeshare -- 事务回滚请求(GlobalRollbackRequest):{}", globalRollback.toString());
        GlobalRollbackResponse response = (GlobalRollbackResponse) syncCall(globalRollback);
        LOGGER.info("ifreeshare -- 事务回滚请求(GlobalRollbackResponse):{}", response.toString());
        return response.getGlobalStatus();
    }

    @Override
    public GlobalStatus getStatus(String xid) throws TransactionException {
        GlobalStatusRequest queryGlobalStatus = new GlobalStatusRequest();
        queryGlobalStatus.setXid(xid);
        LOGGER.info("ifreeshare -- 事务状态查询(GlobalStatusRequest):{}", queryGlobalStatus.toString());
        GlobalStatusResponse response = (GlobalStatusResponse) syncCall(queryGlobalStatus);
        LOGGER.info("ifreeshare -- 事务状态查询(GlobalStatusRequest):{}", response.toString());
        return response.getGlobalStatus();
    }

    @Override
    public GlobalStatus globalReport(String xid, GlobalStatus globalStatus) throws TransactionException {
        GlobalReportRequest globalReport = new GlobalReportRequest();
        globalReport.setXid(xid);
        globalReport.setGlobalStatus(globalStatus);
        LOGGER.info(" 事务上报请求(GlobalReportRequest):{}",globalReport.toString());
        GlobalReportResponse response = (GlobalReportResponse) syncCall(globalReport);
        LOGGER.info("事务上报请求(GlobalReportResponse):{}", response.toString());
        return response.getGlobalStatus();
    }

    private AbstractTransactionResponse syncCall(AbstractTransactionRequest request) throws TransactionException {
        try {
            LOGGER.info("发送请求：request.class:{}",request.getTypeCode());
            LOGGER.info("请求发送, 发送请求 request:{}",
                    request.toString());
            return (AbstractTransactionResponse) TmNettyRemotingClient.getInstance().sendSyncRequest(request);
        } catch (TimeoutException toe) {
            throw new TmTransactionException(TransactionExceptionCode.IO, "RPC timeout", toe);
        }
    }
}
