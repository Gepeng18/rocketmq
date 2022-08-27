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

package org.apache.rocketmq.client.latency;

import org.apache.rocketmq.client.impl.producer.TopicPublishInfo;
import org.apache.rocketmq.client.log.ClientLogger;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.common.message.MessageQueue;

public class MQFaultStrategy {
    private final static InternalLogger log = ClientLogger.getLog();
    private final LatencyFaultTolerance<String> latencyFaultTolerance = new LatencyFaultToleranceImpl();

    private boolean sendLatencyFaultEnable = false;

    private long[] latencyMax = {50L, 100L, 550L, 1000L, 2000L, 3000L, 15000L};
    private long[] notAvailableDuration = {0L, 0L, 30000L, 60000L, 120000L, 180000L, 600000L};

    public long[] getNotAvailableDuration() {
        return notAvailableDuration;
    }

    public void setNotAvailableDuration(final long[] notAvailableDuration) {
        this.notAvailableDuration = notAvailableDuration;
    }

    public long[] getLatencyMax() {
        return latencyMax;
    }

    public void setLatencyMax(final long[] latencyMax) {
        this.latencyMax = latencyMax;
    }

    public boolean isSendLatencyFaultEnable() {
        return sendLatencyFaultEnable;
    }

    public void setSendLatencyFaultEnable(final boolean sendLatencyFaultEnable) {
        this.sendLatencyFaultEnable = sendLatencyFaultEnable;
    }

    /**
     * @param tpInfo 里面包含那一堆MessageQueue
     * @param lastBrokerName 上次选择的 broker name
     * @return
     */
    public MessageQueue selectOneMessageQueue(final TopicPublishInfo tpInfo, final String lastBrokerName) {
        // 发送延迟故障 (默认关闭)
        // 这个其实就是根据你这个broker 的响应延迟时间的大小，来影响下次选择这个broker的权重，他不是绝对的，因为根据它这个规则是在找不出来的话，
        // 他就会使用那套普通选择算法来找个MessageQueue。主要原理为：在每次发送之后都收集一下它这次的一个响应延迟，比如我10点1分1秒200毫秒给broker-a了一个消息，
        // 然后到了10点1分1秒900毫秒的时候才收到broker-a 的一个sendResult也就是响应，这个时候他就是700ms的延迟，它会跟你就这个300ms的延迟找到一个时间范围，
        // 他就认为你这个broker-a 这个broker 在某个时间段内，比如说30s内是不可用的。然后下次选择的时候，他在第一轮会找那些可用的broker，找不到的话，
        // 就找那些上次不是这个broker的，还是找不到的话，他就绝望了，用最普通的方式，也就是selectOneMessageQueue轮询算法找一个MessageQueue出来。
        if (this.sendLatencyFaultEnable) {
            try {
                // 1、依旧产生一个轮训数，找到可用的
                int index = tpInfo.getSendWhichQueue().incrementAndGet();
                for (int i = 0; i < tpInfo.getMessageQueueList().size(); i++) {
                    int pos = Math.abs(index++) % tpInfo.getMessageQueueList().size();
                    if (pos < 0)
                        pos = 0;
                    MessageQueue mq = tpInfo.getMessageQueueList().get(pos);
                    // 1.1 轮询到的MessageQueue可用，直接返回（根据前几次sendMessage时，记录的各个brokerName的可用时间来判断）
                    if (latencyFaultTolerance.isAvailable(mq.getBrokerName()))
                        // 看网上资料，之前还有一串代码：
                        // if (null == lastBrokerName || mq.getBrokerName().equals(lastBrokerName))
                        return mq;
                }

                // 2. 没找到一个可用的，就尝试选择一个距离可用时间最近的
                final String notBestBroker = latencyFaultTolerance.pickOneAtLeast();
                // 找到这个broker后，根据brokerName获取写队列的个数，其实写队列个数有几个，然后broker对应的MessageQueue就有几个
                int writeQueueNums = tpInfo.getQueueIdByBroker(notBestBroker);
                // 如果写队列个数大于0，就轮询选一个MessageQueue，然后设置它的broker name 与queue id
                if (writeQueueNums > 0) {
                    final MessageQueue mq = tpInfo.selectOneMessageQueue();
                    if (notBestBroker != null) {
                        // 重新设置messageQueue的brokerName和queueId
                        mq.setBrokerName(notBestBroker);
                        mq.setQueueId(tpInfo.getSendWhichQueue().incrementAndGet() % writeQueueNums);
                    }
                    return mq;
                } else {
                    // 如果write<=0，直接移除这个broker对应FaultItem，我觉得这里有个问题。
                    // 作者原意应该是，移出latencyFaultTolerance，这样后面执行代码时，上面的第二步就不会再选出这个broker了，
                    // 但是，如果把broker移除了，那下次broker就代表可用的了，那还是会被第一步选中啊，因为第一步中，当轮询选了一个brokerName后，
                    // 会根据latencyFaultTolerance判断是否可用，如果这个里面没有这个broker，表明之前没出现过延迟，所以直接return true，
                    // 就代表被选中了，所以这里没看懂。
                    latencyFaultTolerance.remove(notBestBroker);
                }
            } catch (Exception e) {
                log.error("Error occurred when selecting message queue", e);
            }

            return tpInfo.selectOneMessageQueue();
        }

        // 上面的默认是关闭的，所以我们先来看下最普通的选择策略，可以看到调用了TopicPublishInfo 的selectOneMessageQueue方法
        return tpInfo.selectOneMessageQueue(lastBrokerName);
    }

    /**
     * @param isolation 发送异常时，置为true，否则为false
     */
    public void updateFaultItem(final String brokerName, final long currentLatency, boolean isolation) {
        // 开启了延迟故障容错，默认是关闭的
        if (this.sendLatencyFaultEnable) {
            // 计算不可用的持续时间，根据本次message的延迟时间进行计算，如果熔断，按照30000计算，否则按照真正的延迟时间计算
            long duration = computeNotAvailableDuration(isolation ? 30000 : currentLatency);
            // 这个方法其实就是用来存储的
            this.latencyFaultTolerance.updateFaultItem(brokerName, currentLatency, duration);
        }
    }

    /**
     * 计算不可用持续时间
     * @param currentLatency 当前延迟，熔断的话就配置30000， 否则的话就是正常的那个响应时间
     * @return
     */
    private long computeNotAvailableDuration(final long currentLatency) {
        // 响应延迟数组和不可使用数组是一一对应的
        // 延迟大于某个时间，就找对应下标的不可使用的时间, 比如说响应延迟700ms，这时候他就会找到30000不可使用时间。
        //  {50L, 100L, 550L, 1000L, 2000L, 3000L, 15000L};
        for (int i = latencyMax.length - 1; i >= 0; i--) {
            if (currentLatency >= latencyMax[i])
                // {0L, 0L, 30000L, 60000L, 120000L, 180000L, 600000L}
                return this.notAvailableDuration[i];
        }

        return 0;
    }
}
