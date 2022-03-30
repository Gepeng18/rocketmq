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
package org.apache.rocketmq.client.consumer;

import java.util.List;
import org.apache.rocketmq.common.message.MessageQueue;

/**
 * Strategy Algorithm for message allocating between consumers
 */
public interface AllocateMessageQueueStrategy {

    /**
     * Allocating by consumer id
     * 先用 队列的数量% 消费者数量，这个其实就是想看看 这几个消费者能不能均分这几个队列，不能的话还剩几个零头的队列，接着就是计算你自己能分几个，如果能均分的话，你跟别人都一样，如果不能均分的话，就看看你在消费者里面哪个位置（其实这里就体现出第二步排序的重要性了），如果你的位置在零头个数的前头的话，就还能分一个队列，这里的核心就是如果能均分就均分，不能均分的话看看还剩几个队列，比如剩3个把，就让排在前面的三个消费者实例一人一个。
     * 接下来就是判断从哪个位置开始取了，比如说我现在有8个queue，然后3个消费者实例分别是0，1，2，0是自己，然后按照上面的算法，消费者0能分3个queue，消费者1也能分3个queue，消费者2分2个queue，
     * 接着就是计算从queue集合取的位置了，根据上面的那个公式算出来，消费者0 是从0 开始取，消费者1是从3开始取，消费者2是从6开始取，然后计算出取的范围，取的范围与你分几个queue有很大的关系
     * 接着就是具体的取了，如果是消费者0的话就是从0开始取，取3个，然后消费者1的话从3开始取往后取3个，消费者2的话从6开始去取2个
     * 仔细体会一下，如果排序的话，会是什么情况，可能会分到重复的queue。这个时候，我自己是消费者0 ，我就可以把queue1，2，3 返回去了。
     * @param consumerGroup current consumer group
     * @param currentCID current consumer id
     * @param mqAll message queue set in current topic
     * @param cidAll consumer set in current consumer group
     * @return The allocate result of given strategy
     */
    List<MessageQueue> allocate(
        final String consumerGroup,
        final String currentCID,
        final List<MessageQueue> mqAll,
        final List<String> cidAll
    );

    /**
     * Algorithm name
     *
     * @return The strategy name
     */
    String getName();
}
