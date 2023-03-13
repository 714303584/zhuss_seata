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
package io.seata.server;

import java.io.IOException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import io.seata.common.XID;
import io.seata.common.thread.NamedThreadFactory;
import io.seata.common.util.NetUtil;
import io.seata.core.constants.ConfigurationKeys;
import io.seata.core.rpc.ShutdownHook;
import io.seata.core.rpc.netty.NettyRemotingServer;
import io.seata.core.rpc.netty.NettyServerConfig;
import io.seata.server.coordinator.DefaultCoordinator;
import io.seata.server.env.ContainerHelper;
import io.seata.server.env.PortHelper;
import io.seata.server.metrics.MetricsManager;
import io.seata.server.session.SessionHolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The type Server.
 *
 * @author slievrly
 */
public class Server {
    /**
     * The entry point of application.
     * TC的启动入口
     *  seata服务的启动入口
     * @param args the input arguments
     * @throws IOException the io exception
     */
    public static void main(String[] args) throws IOException {
        // get port first, use to logback.xml
        //获取port
        int port = PortHelper.getPort(args);
        System.setProperty(ConfigurationKeys.SERVER_PORT, Integer.toString(port));



        // create logger
        final Logger logger = LoggerFactory.getLogger(Server.class);


        logger.info("server port:{}",Integer.toString(port));
        if (ContainerHelper.isRunningInContainer()) {
            logger.info("The server is running in container.");
        }

        //initialize the parameter parser
        //初始化参数解析
        //Note that the parameter parser should always be the first line to execute.
        //Because, here we need to parse the parameters needed for startup.
        ParameterParser parameterParser = new ParameterParser(args);

        //initialize the metrics
        //指标管理器初始化
        MetricsManager.get().init();
        //获取存储方式
        System.setProperty(ConfigurationKeys.STORE_MODE, parameterParser.getStoreMode());

        //工作线程池初始化
        //工作线程池-- 参数
        //从nettserverconfig中获取
        ThreadPoolExecutor workingThreads = new ThreadPoolExecutor(NettyServerConfig.getMinServerPoolSize(),
                NettyServerConfig.getMaxServerPoolSize(), NettyServerConfig.getKeepAliveTime(), TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(NettyServerConfig.getMaxTaskQueueSize()),
                new NamedThreadFactory("ServerHandlerThread", NettyServerConfig.getMaxServerPoolSize()), new ThreadPoolExecutor.CallerRunsPolicy());

        //初始化事务协调器服务
        NettyRemotingServer nettyRemotingServer = new NettyRemotingServer(workingThreads);
        //server port
        //设置端口
        nettyRemotingServer.setListenPort(parameterParser.getPort());
        //设置TC的唯一标志
        UUIDGenerator.init(parameterParser.getServerNode());
        //log store mode : file, db, redis
        //日志记录方式
        SessionHolder.init(parameterParser.getStoreMode());
        //默认的事务协调器 并指定通信服务模块
        DefaultCoordinator coordinator = new DefaultCoordinator(nettyRemotingServer);
        //进行事务协调器初始化 -- {{{重点}}}
        coordinator.init();
        //服务端设置处理器
        //注意事务协调器实现了
        nettyRemotingServer.setHandler(coordinator);
        // register ShutdownHook
        //注册关闭钩子
        ShutdownHook.getInstance().addDisposable(coordinator);
        ShutdownHook.getInstance().addDisposable(nettyRemotingServer);

        //127.0.0.1 and 0.0.0.0 are not valid here.
        if (NetUtil.isValidIp(parameterParser.getHost(), false)) {
            XID.setIpAddress(parameterParser.getHost());
        } else {
            XID.setIpAddress(NetUtil.getLocalIp());
        }
        XID.setPort(nettyRemotingServer.getListenPort());

        try {
            nettyRemotingServer.init();
        } catch (Throwable e) {
            logger.error("nettyServer init error:{}", e.getMessage(), e);
            System.exit(-1);
        }

        System.exit(0);
    }
}
