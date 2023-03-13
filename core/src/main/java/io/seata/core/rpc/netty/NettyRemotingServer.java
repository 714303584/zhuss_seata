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
package io.seata.core.rpc.netty;

import io.netty.channel.Channel;
import io.seata.core.protocol.MessageType;
import io.seata.core.rpc.TransactionMessageHandler;
import io.seata.core.rpc.processor.server.RegRmProcessor;
import io.seata.core.rpc.processor.server.RegTmProcessor;
import io.seata.core.rpc.processor.server.ServerHeartbeatProcessor;
import io.seata.core.rpc.processor.server.ServerOnRequestProcessor;
import io.seata.core.rpc.processor.server.ServerOnResponseProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * The netty remoting server.
 *
 * @author slievrly
 * @author xingfudeshi@gmail.com
 * @author zhangchenghui.dev@gmail.com
 */
public class NettyRemotingServer extends AbstractNettyRemotingServer {

    private static final Logger LOGGER = LoggerFactory.getLogger(NettyRemotingServer.class);

    private TransactionMessageHandler transactionMessageHandler;

    private final AtomicBoolean initialized = new AtomicBoolean(false);

    /**
     * 进行服务端初始化
     */
    @Override
    public void init() {
        // registry processor
        registerProcessor();
        if (initialized.compareAndSet(false, true)) {
            super.init();
        }
    }

    /**
     * Instantiates a new Rpc remoting server.
     *
     * @param messageExecutor   the message executor
     */
    public NettyRemotingServer(ThreadPoolExecutor messageExecutor) {
        super(messageExecutor, new NettyServerConfig());
    }

    /**
     * Sets transactionMessageHandler.
     *
     * @param transactionMessageHandler the transactionMessageHandler
     */
    public void setHandler(TransactionMessageHandler transactionMessageHandler) {
        this.transactionMessageHandler = transactionMessageHandler;
    }

    public TransactionMessageHandler getHandler() {
        return transactionMessageHandler;
    }

    @Override
    public void destroyChannel(String serverAddress, Channel channel) {
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("will destroy channel:{},address:{}", channel, serverAddress);
        }
        channel.disconnect();
        channel.close();
    }

    /**
     * 注册处理器
     *      根据远程调用的消息类型进行消息处理
     *       TC消息处理
     */
    private void registerProcessor() {
        // 1. registry on request message processor
        ServerOnRequestProcessor onRequestProcessor =
            new ServerOnRequestProcessor(this, getHandler());
        //分支事务注册消息处理
        super.registerProcessor(MessageType.TYPE_BRANCH_REGISTER, onRequestProcessor, messageExecutor);
        //分支事务状态上报
        super.registerProcessor(MessageType.TYPE_BRANCH_STATUS_REPORT, onRequestProcessor, messageExecutor);
        //全局事务开始消息处理器
        super.registerProcessor(MessageType.TYPE_GLOBAL_BEGIN, onRequestProcessor, messageExecutor);
        //全局事务提交消息处理
        super.registerProcessor(MessageType.TYPE_GLOBAL_COMMIT, onRequestProcessor, messageExecutor);
        //全局锁查询消息处理
        super.registerProcessor(MessageType.TYPE_GLOBAL_LOCK_QUERY, onRequestProcessor, messageExecutor);
        //全局上报消息处理注册
        super.registerProcessor(MessageType.TYPE_GLOBAL_REPORT, onRequestProcessor, messageExecutor);
        //全局回滚消息处理
        super.registerProcessor(MessageType.TYPE_GLOBAL_ROLLBACK, onRequestProcessor, messageExecutor);
        //全局状态消息
        super.registerProcessor(MessageType.TYPE_GLOBAL_STATUS, onRequestProcessor, messageExecutor);
        //seata合并消息
        super.registerProcessor(MessageType.TYPE_SEATA_MERGE, onRequestProcessor, messageExecutor);
        // 2. registry on response message processor
        ServerOnResponseProcessor onResponseProcessor =
            new ServerOnResponseProcessor(getHandler(), getFutures());
        //分支事务提交结果
        super.registerProcessor(MessageType.TYPE_BRANCH_COMMIT_RESULT, onResponseProcessor, messageExecutor);
        //分支事务回滚结果
        super.registerProcessor(MessageType.TYPE_BRANCH_ROLLBACK_RESULT, onResponseProcessor, messageExecutor);
        // 3. registry rm message processor
        //rm注册消息处理
        RegRmProcessor regRmProcessor = new RegRmProcessor(this);
        super.registerProcessor(MessageType.TYPE_REG_RM, regRmProcessor, messageExecutor);
        // 4. registry tm message processor
        //tm注册消息处理
        RegTmProcessor regTmProcessor = new RegTmProcessor(this);
        super.registerProcessor(MessageType.TYPE_REG_CLT, regTmProcessor, null);
        // 5. registry heartbeat message processor
        // 注册心跳消息处理
        ServerHeartbeatProcessor heartbeatMessageProcessor = new ServerHeartbeatProcessor(this);
        super.registerProcessor(MessageType.TYPE_HEARTBEAT_MSG, heartbeatMessageProcessor, null);
    }

}
