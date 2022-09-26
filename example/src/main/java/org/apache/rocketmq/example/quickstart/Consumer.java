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
package org.apache.rocketmq.example.quickstart;

import java.util.List;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;

/**
 * This example shows how to subscribe and consume messages using providing {@link DefaultMQPushConsumer}.
 */
public class Consumer {

    public static void main(String[] args) throws InterruptedException, MQClientException {

        /*
         * Instantiate with specified consumer group name.
         * 创建一个push模式的consumer对象
         */
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("please_rename_unique_group_name_4");

        consumer.setNamesrvAddr("127.0.0.1:9876");

        /*
         * Specify name server addresses.
         * <p/>
         *
         * Alternatively, you may specify name server addresses via exporting environmental variable: NAMESRV_ADDR
         * <pre>
         * {@code
         * consumer.setNamesrvAddr("name-server1-ip:9876;name-server2-ip:9876");
         * }
         * </pre>
         */

        /*
         * Specify where to start in case the specific consumer group is a brand-new one.
         * 设置从哪里开始消费
         * 当建立一个新的消费者组时，需要决定是否需要消费已经存在于 Broker 中的历史消息
         * CONSUME_FROM_LAST_OFFSET 将会忽略历史消息，并消费之后生成的任何消息。
         * CONSUME_FROM_FIRST_OFFSET 将会消费每个存在于 Broker 中的信息。
         * 你也可以使用 CONSUME_FROM_TIMESTAMP 来消费在指定时间戳后产生的消息
         */
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);

        /*
         * 订阅主题
         */
        consumer.subscribe("TopicTest", "*");

        /*
         * Register callback to execute on arrival of messages fetched from brokers.
         * 注册回调，消息到达的时候执行
         * MessageListenerConcurrently 这个有两个实现类，一个是MessageListenerConcurrently，也就是咱们例子那个，
         * 从名字就可以看出来是个并发消费的，消息到了的时候，不管消息消费顺序，然后并发消费，如果你业务场景要求顺序消息的话，
         * 就不能选择这个了，需要使用它另一个实现类MessageListenerOrderly ，这个能够保证消息顺序消费，
         * 它其实就是一个线程来一个消息一个消息消费的。
         */
        consumer.registerMessageListener(new MessageListenerConcurrently() {

            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs,
                ConsumeConcurrentlyContext context) {
                System.out.printf("%s Receive New Messages: %s %n", Thread.currentThread().getName(), msgs);
                // 执行状态的返回
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });

        /*
         *  Launch the consumer instance.
         */
        consumer.start();

        System.out.printf("Consumer Started.%n");
    }
}
