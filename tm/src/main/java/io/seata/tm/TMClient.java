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

import io.seata.core.rpc.netty.TmNettyRemotingClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The type Tm client.
 *
 * @author slievrly
 */
public class TMClient {

    private static final Logger LOGGER = LoggerFactory.getLogger(TMClient.class);


    /**
     * Init.
     *
     * @param applicationId           the application id
     * @param transactionServiceGroup the transaction service group
     */
    public static void init(String applicationId, String transactionServiceGroup) {
        LOGGER.info("初始化事务管理器客户端 TmClient.init()");
        init(applicationId, transactionServiceGroup, null, null);
    }

    /**
     * Init.
     *
     * @param applicationId           the application id
     * @param transactionServiceGroup the transaction service group
     * @param accessKey               the access key
     * @param secretKey               the secret key
     */
    public static void init(String applicationId, String transactionServiceGroup, String accessKey, String secretKey) {
        LOGGER.info("初始化事务管理器客户端 TmClient.init(),applicationId-{},transactionServiceGroup-{},secretKey-{}",
                applicationId,transactionServiceGroup,secretKey);
        TmNettyRemotingClient tmNettyRemotingClient = TmNettyRemotingClient.getInstance(applicationId, transactionServiceGroup, accessKey, secretKey);
        tmNettyRemotingClient.init();
    }

}
