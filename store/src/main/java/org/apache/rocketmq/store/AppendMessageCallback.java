/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.store;

import java.nio.ByteBuffer;
import org.apache.rocketmq.common.message.MessageExtBatch;
import org.apache.rocketmq.store.CommitLog.PutMessageContext;

/**
 * Write messages callback interface
 */
public interface AppendMessageCallback {

    /**
     * After message serialization, write MapedByteBuffer
     * 第一个参数是开始offset，这个offset是commitlog的一个offset，举个例子，第一个MappedFile的开始offset是0，然后一个MappedFile 的大小是1g，
     * 那第二个MappedFile就得从1073741824（1g）开始了，第二个参数是bytebuffer，这个不用多说，第三个是这个MappedFile还空多少字节没用，第四个就是消息了。
     * @return How many bytes to write
     */
    AppendMessageResult doAppend(final long fileFromOffset, final ByteBuffer byteBuffer,
        final int maxBlank, final MessageExtBrokerInner msg, PutMessageContext putMessageContext);

    /**
     * After batched message serialization, write MapedByteBuffer
     *
     * @param messageExtBatch, backed up by a byte array
     * @return How many bytes to write
     */
    AppendMessageResult doAppend(final long fileFromOffset, final ByteBuffer byteBuffer,
        final int maxBlank, final MessageExtBatch messageExtBatch, PutMessageContext putMessageContext);
}
