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
package io.seata.core.rpc.netty.v1;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;
import io.seata.common.loader.EnhancedServiceLoader;
import io.seata.core.serializer.Serializer;
import io.seata.core.compressor.Compressor;
import io.seata.core.compressor.CompressorFactory;
import io.seata.core.protocol.ProtocolConstants;
import io.seata.core.protocol.RpcMessage;
import io.seata.core.serializer.SerializerType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * <pre>
 * 0     1     2     3     4     5     6     7     8     9    10     11    12    13    14    15    16
 * +-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+
 * |   magic   |Proto|     Full length       |    Head   | Msg |Seria|Compr|     RequestId         |
 * |   code    |colVer|    (head+body)      |   Length  |Type |lizer|ess  |                       |
 * +-----------+-----------+-----------+-----------+-----------+-----------+-----------+-----------+
 * |                                                                                               |
 * |                                   Head Map [Optional]                                         |
 * +-----------+-----------+-----------+-----------+-----------+-----------+-----------+-----------+
 * |                                                                                               |
 * |                                         body                                                  |
 * |                                                                                               |
 * |                                        ... ...                                                |
 * +-----------------------------------------------------------------------------------------------+
 * </pre>
 * <p>
 * <li>Full Length: include all data </li>
 * <li>Head Length: include head data from magic code to head map. </li>
 * <li>Body Length: Full Length - Head Length</li>
 * </p>
 * https://github.com/seata/seata/issues/893
 * 进行消息包的编码 -- 将seata的netty消息进行编码打包
 *
 * @author Geng Zhang
 * @see ProtocolV1Decoder
 * @since 0.7.0
 */
public class ProtocolV1Encoder extends MessageToByteEncoder {

    private static final Logger LOGGER = LoggerFactory.getLogger(ProtocolV1Encoder.class);

    @Override
    public void encode(ChannelHandlerContext ctx, Object msg, ByteBuf out) {
        try {
            //消息为seata的rpc消息
            if (msg instanceof RpcMessage) {
                //转换为Rpc消息
                RpcMessage rpcMessage = (RpcMessage) msg;

                //获取总长度
                //默认总长度
                int fullLength = ProtocolConstants.V1_HEAD_LENGTH;
                //获取头长度
                int headLength = ProtocolConstants.V1_HEAD_LENGTH;

                //获取消息类型
                byte messageType = rpcMessage.getMessageType();
                //写入
                out.writeBytes(ProtocolConstants.MAGIC_CODE_BYTES);
                //写入协议版本
                out.writeByte(ProtocolConstants.VERSION);
                // full Length(4B) and head length(2B) will fix in the end. 
                out.writerIndex(out.writerIndex() + 6);
                out.writeByte(messageType);
                //写入Codec
                out.writeByte(rpcMessage.getCodec());
                //写入压缩方式
                out.writeByte(rpcMessage.getCompressor());
                //写入id
                out.writeInt(rpcMessage.getId());

                // direct write head with zero-copy
                //写入消息头
                Map<String, String> headMap = rpcMessage.getHeadMap();
                if (headMap != null && !headMap.isEmpty()) {
                    //编码消息头
                    int headMapBytesLength = HeadMapSerializer.getInstance().encode(headMap, out);
                    //将消息头的长度加入
                    headLength += headMapBytesLength;
                    fullLength += headMapBytesLength;
                }

                byte[] bodyBytes = null;
                //不为心跳请求 -- 心跳请求没有消息体
                if (messageType != ProtocolConstants.MSGTYPE_HEARTBEAT_REQUEST
                        && messageType != ProtocolConstants.MSGTYPE_HEARTBEAT_RESPONSE) {
                    // heartbeat has no body
                    //获取序列化方法
                    Serializer serializer = EnhancedServiceLoader.load(Serializer.class, SerializerType.getByCode(rpcMessage.getCodec()).name());
                    //序列化消息体
                    bodyBytes = serializer.serialize(rpcMessage.getBody());
                    //获取压缩机
                    Compressor compressor = CompressorFactory.getCompressor(rpcMessage.getCompressor());
                    //进行压缩
                    bodyBytes = compressor.compress(bodyBytes);
                    fullLength += bodyBytes.length;
                }

                //写入输出
                if (bodyBytes != null) {
                    out.writeBytes(bodyBytes);
                }

                // fix fullLength and headLength
                int writeIndex = out.writerIndex();
                // skip magic code(2B) + version(1B)
                out.writerIndex(writeIndex - fullLength + 3);
                out.writeInt(fullLength);
                out.writeShort(headLength);
                out.writerIndex(writeIndex);
            } else {
                throw new UnsupportedOperationException("Not support this class:" + msg.getClass());
            }
        } catch (Throwable e) {
            LOGGER.error("Encode request error!", e);
        }
    }
}
