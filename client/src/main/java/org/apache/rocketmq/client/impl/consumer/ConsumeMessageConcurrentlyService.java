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
package org.apache.rocketmq.client.impl.consumer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.ConsumeReturnType;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.hook.ConsumeMessageContext;
import org.apache.rocketmq.client.log.ClientLogger;
import org.apache.rocketmq.client.stat.ConsumerStatsManager;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.body.CMResult;
import org.apache.rocketmq.common.protocol.body.ConsumeMessageDirectlyResult;
import org.apache.rocketmq.common.utils.ThreadUtils;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.remoting.common.RemotingHelper;

public class ConsumeMessageConcurrentlyService implements ConsumeMessageService {
    private static final InternalLogger log = ClientLogger.getLog();
    private final DefaultMQPushConsumerImpl defaultMQPushConsumerImpl;
    private final DefaultMQPushConsumer defaultMQPushConsumer;
    private final MessageListenerConcurrently messageListener;
    private final BlockingQueue<Runnable> consumeRequestQueue;
    private final ThreadPoolExecutor consumeExecutor;
    private final String consumerGroup;

    private final ScheduledExecutorService scheduledExecutorService;
    private final ScheduledExecutorService cleanExpireMsgExecutors;

    public ConsumeMessageConcurrentlyService(DefaultMQPushConsumerImpl defaultMQPushConsumerImpl,
        MessageListenerConcurrently messageListener) {
        this.defaultMQPushConsumerImpl = defaultMQPushConsumerImpl;
        this.messageListener = messageListener;

        this.defaultMQPushConsumer = this.defaultMQPushConsumerImpl.getDefaultMQPushConsumer();
        this.consumerGroup = this.defaultMQPushConsumer.getConsumerGroup();
        this.consumeRequestQueue = new LinkedBlockingQueue<Runnable>();

        String consumeThreadPrefix = null;
        if (consumerGroup.length() > 100) {
            consumeThreadPrefix = new StringBuilder("ConsumeMessageThread_").append(consumerGroup.substring(0, 100)).append("_").toString();
        } else {
            consumeThreadPrefix = new StringBuilder("ConsumeMessageThread_").append(consumerGroup).append("_").toString();
        }
        this.consumeExecutor = new ThreadPoolExecutor(
            this.defaultMQPushConsumer.getConsumeThreadMin(),
            this.defaultMQPushConsumer.getConsumeThreadMax(),
            1000 * 60,
            TimeUnit.MILLISECONDS,
            this.consumeRequestQueue,
            new ThreadFactoryImpl(consumeThreadPrefix));

        this.scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryImpl("ConsumeMessageScheduledThread_"));
        this.cleanExpireMsgExecutors = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryImpl("CleanExpireMsgScheduledThread_"));
    }

    public void start() {
        this.cleanExpireMsgExecutors.scheduleAtFixedRate(new Runnable() {

            @Override
            public void run() {
                try {
                    // 清理过期消息
                    cleanExpireMsg();
                } catch (Throwable e) {
                    log.error("scheduleAtFixedRate cleanExpireMsg exception", e);
                }
                // 15分钟执行一次
            }

        }, this.defaultMQPushConsumer.getConsumeTimeout(), this.defaultMQPushConsumer.getConsumeTimeout(), TimeUnit.MINUTES);
    }

    public void shutdown(long awaitTerminateMillis) {
        this.scheduledExecutorService.shutdown();
        ThreadUtils.shutdownGracefully(this.consumeExecutor, awaitTerminateMillis, TimeUnit.MILLISECONDS);
        this.cleanExpireMsgExecutors.shutdown();
    }

    @Override
    public void updateCorePoolSize(int corePoolSize) {
        if (corePoolSize > 0
            && corePoolSize <= Short.MAX_VALUE
            && corePoolSize < this.defaultMQPushConsumer.getConsumeThreadMax()) {
            this.consumeExecutor.setCorePoolSize(corePoolSize);
        }
    }

    @Override
    public void incCorePoolSize() {

    }

    @Override
    public void decCorePoolSize() {

    }

    @Override
    public int getCorePoolSize() {
        return this.consumeExecutor.getCorePoolSize();
    }

    @Override
    public ConsumeMessageDirectlyResult consumeMessageDirectly(MessageExt msg, String brokerName) {
        ConsumeMessageDirectlyResult result = new ConsumeMessageDirectlyResult();
        result.setOrder(false);
        result.setAutoCommit(true);

        List<MessageExt> msgs = new ArrayList<MessageExt>();
        msgs.add(msg);
        MessageQueue mq = new MessageQueue();
        mq.setBrokerName(brokerName);
        mq.setTopic(msg.getTopic());
        mq.setQueueId(msg.getQueueId());

        ConsumeConcurrentlyContext context = new ConsumeConcurrentlyContext(mq);

        this.defaultMQPushConsumerImpl.resetRetryAndNamespace(msgs, this.consumerGroup);

        final long beginTime = System.currentTimeMillis();

        log.info("consumeMessageDirectly receive new message: {}", msg);

        try {
            ConsumeConcurrentlyStatus status = this.messageListener.consumeMessage(msgs, context);
            if (status != null) {
                switch (status) {
                    case CONSUME_SUCCESS:
                        result.setConsumeResult(CMResult.CR_SUCCESS);
                        break;
                    case RECONSUME_LATER:
                        result.setConsumeResult(CMResult.CR_LATER);
                        break;
                    default:
                        break;
                }
            } else {
                result.setConsumeResult(CMResult.CR_RETURN_NULL);
            }
        } catch (Throwable e) {
            result.setConsumeResult(CMResult.CR_THROW_EXCEPTION);
            result.setRemark(RemotingHelper.exceptionSimpleDesc(e));

            log.warn(String.format("consumeMessageDirectly exception: %s Group: %s Msgs: %s MQ: %s",
                RemotingHelper.exceptionSimpleDesc(e),
                ConsumeMessageConcurrentlyService.this.consumerGroup,
                msgs,
                mq), e);
        }

        result.setSpentTimeMills(System.currentTimeMillis() - beginTime);

        log.info("consumeMessageDirectly Result: {}", result);

        return result;
    }

    /**
     * 这一块就是拉取回来的消息个数正好在你批量消费范围之内的话，就直接封装ConsumeRequst，提交给线程池处理，
     * 如果是拉取回来的消息个数大于你批量消费范围的话，就分批封装成ConsumeRequst，扔给线程池处理，默认批量消费是1个，
     * 如果你拉回来32个消息，他就会给你封装成ConsumeRequst对象，扔到线程池中并发消费， 注意这个线程池core是20 ，maxCore是60 ，然后队列没有限制大小。
     */
    @Override
    public void submitConsumeRequest(
        final List<MessageExt> msgs,
        final ProcessQueue processQueue,
        final MessageQueue messageQueue,
        final boolean dispatchToConsume) {
        // ipt 将msgs中的消息分成若干批，每一批使用一个线程进行消费
        // 批量消费规模 默认是1个，所以现在的参数下，一批消息，每个消息一个线程进行执行
        final int consumeBatchSize = this.defaultMQPushConsumer.getConsumeMessageBatchMaxSize();
        if (msgs.size() <= consumeBatchSize) {//消息正好在批量消费范围内
            //封装消费请求
            ConsumeRequest consumeRequest = new ConsumeRequest(msgs, processQueue, messageQueue);
            try {
                // 提交消费
                this.consumeExecutor.submit(consumeRequest);
            } catch (RejectedExecutionException e) {
                this.submitConsumeRequestLater(consumeRequest);
            }
        } else {
            for (int total = 0; total < msgs.size(); ) {
                List<MessageExt> msgThis = new ArrayList<MessageExt>(consumeBatchSize);
                for (int i = 0; i < consumeBatchSize; i++, total++) {
                    if (total < msgs.size()) {
                        msgThis.add(msgs.get(total));
                    } else {
                        break;
                    }
                }

                // 将msgs消息分批，每批组成一个 consumeRequest，放到线程池中由一个线程进行执行
                ConsumeRequest consumeRequest = new ConsumeRequest(msgThis, processQueue, messageQueue);
                try {
                    // 提交到线程池处理
                    this.consumeExecutor.submit(consumeRequest);
                } catch (RejectedExecutionException e) {
                    // 消费不过来的时候 这玩意出现不了啊,上面的消费队列大小没有限定
                    for (; total < msgs.size(); total++) {
                        msgThis.add(msgs.get(total));
                    }

                    this.submitConsumeRequestLater(consumeRequest);
                }
            }
        }
    }


    private void cleanExpireMsg() {
        Iterator<Map.Entry<MessageQueue, ProcessQueue>> it =
            this.defaultMQPushConsumerImpl.getRebalanceImpl().getProcessQueueTable().entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<MessageQueue, ProcessQueue> next = it.next();
            ProcessQueue pq = next.getValue();
            pq.cleanExpiredMsg(this.defaultMQPushConsumer);
        }
    }

    /**
     *
     * @param status 本批的消费结果
     * @param context
     * @param consumeRequest 本批的待消费的msg
     *
     * 1、记录各种指标
     * 2、集群模式下，如果本批消费结果为later，则将这批消息发回broker，如果发送失败，则组成一个list，然后本地进行消费
     * 3、更新offset，从pq中删除所有消费失败且发回broker成功的msg（这两个条件表明这批消息后面还会从broker进行分发），然后pq中最小的offset即为需要更新的offset
     */
    public void processConsumeResult(
        final ConsumeConcurrentlyStatus status,
        final ConsumeConcurrentlyContext context,
        final ConsumeRequest consumeRequest
    ) {
        /**
         * 这里这一段就是计算成功几个，然后失败几个的，这个ackIndex，你可以在消费的时候根据实际成功情况来确认ack，
         * 当然你也可以不用设置，只要你消费的时候异常不往上抛，它就认为你消费成功了，一旦让它捕获到异常，你这一批消费都要被放到重试队列中。
         */

        // 获取ack
        int ackIndex = context.getAckIndex();

        if (consumeRequest.getMsgs().isEmpty())
            return;

        switch (status) {
            case CONSUME_SUCCESS:
                if (ackIndex >= consumeRequest.getMsgs().size()) { // 如果ackIndex >=消费信息集合大小的话
                    // ack 索引位置
                    // 设置成 消费信息集合大小-1
                    ackIndex = consumeRequest.getMsgs().size() - 1;
                }
                // 成功的个数，这里就是消息总个数
                int ok = ackIndex + 1;
                // 失败的
                int failed = consumeRequest.getMsgs().size() - ok;
                // 记录成功的TPS
                this.getConsumerStatsManager().incConsumeOKTPS(consumerGroup, consumeRequest.getMessageQueue().getTopic(), ok);
                // 记录失败的TPS
                this.getConsumerStatsManager().incConsumeFailedTPS(consumerGroup, consumeRequest.getMessageQueue().getTopic(), failed);
                break;
            case RECONSUME_LATER:
                ackIndex = -1;
                this.getConsumerStatsManager().incConsumeFailedTPS(consumerGroup, consumeRequest.getMessageQueue().getTopic(),
                    consumeRequest.getMsgs().size());
                break;
            default:
                break;
        }

        switch (this.defaultMQPushConsumer.getMessageModel()) { // 消费类型
            case BROADCASTING:
                // 广播，打印一堆消息消费失败的日志
                for (int i = ackIndex + 1; i < consumeRequest.getMsgs().size(); i++) {
                    MessageExt msg = consumeRequest.getMsgs().get(i);
                    log.warn("BROADCASTING, the message consume failed, drop it, {}", msg.toString());
                }
                break;
            case CLUSTERING: // 集群
                /**
                 * 这一块就是根据这个ackIndex，往后的消息它认为是失败的，然后发给broker 的重试队列中，进行重试消费，
                 * 如果发送重试消息失败就加到集合中，等一会在本地再消费一下。
                 * 这里对于稍后消费，ackIndex = -1，所以表示这一批都需要重新发给broker的重试队列
                 */
                // 这是本地重试队列，如果发送给broker的重试队列失败，则放到该list中本地重试
                List<MessageExt> msgBackFailed = new ArrayList<MessageExt>(consumeRequest.getMsgs().size());
                for (int i = ackIndex + 1; i < consumeRequest.getMsgs().size(); i++) {
                    MessageExt msg = consumeRequest.getMsgs().get(i);
                    // sendMessageBack：将失败的消息交给重试队列
                    boolean result = this.sendMessageBack(msg, context);
                    if (!result) {
                        msg.setReconsumeTimes(msg.getReconsumeTimes() + 1); // 重新消费次数 + 1
                        msgBackFailed.add(msg);
                    }
                }

                if (!msgBackFailed.isEmpty()) {
                    consumeRequest.getMsgs().removeAll(msgBackFailed);
                    // consumeRequest的msgs中剩下发送给broker成功的消息，因为发给broker失败的消息已经走本地逻辑了，不会再走broker分发逻辑

                    // 需要将这些失败的稍后再进行下消费
                    this.submitConsumeRequestLater(msgBackFailed, consumeRequest.getProcessQueue(), consumeRequest.getMessageQueue());
                }
                break;
            default:
                break;
        }

        // 这个offset 其实就是消费到哪的一个offset,然后如果这个treeMap里面还有数据的话,就返回那个最小的offset ,如果这个treeMap空了的话,就是offsetMax+1
        // 这里实际就是在processQueue的treeMap中删除消费完的消息，获取一个最小的offset，更新本地对应的消费offset
        // 稍后消费的情况下：consumeRequest的msgs中剩下一批消息，这批消息是没处理成功的，pq中消息去掉这部分，就是处理成功的msg了，
        // 然后拿出这批处理成功的msg中的最小值，就是当前offset了，在当前实现下，消费失败代表本批全部失败，并且假定消息都发回broker成功了，
        // 这时候 consumeRequest.getMsgs() 代表本批全部数据，然后pq.remove表示去掉本批数据，然后获取的offset表示不带本批数据的最小offset
        long offset = consumeRequest.getProcessQueue().removeMessage(consumeRequest.getMsgs());
        if (offset >= 0 && !consumeRequest.getProcessQueue().isDropped()) {
            // 更新一下这个offset
            this.defaultMQPushConsumerImpl.getOffsetStore().updateOffset(consumeRequest.getMessageQueue(), offset, true);
        }
    }

    public ConsumerStatsManager getConsumerStatsManager() {
        return this.defaultMQPushConsumerImpl.getConsumerStatsManager();
    }

    /**
     *  这个就是将失败的消息发送到broker的重试队列中，可以看到有个延迟等级，
     *  这个其实就是根据你的重试次数进行延迟消费的，这里默认的延迟等级是0，
     *  但是到broker端，它发现你传过来的是0，它会给你设置成3+重试次数
     */
    public boolean sendMessageBack(final MessageExt msg, final ConsumeConcurrentlyContext context) {
        // 获取延迟等级
        int delayLevel = context.getDelayLevelWhenNextConsume();

        // Wrap topic with namespace before sending back message.
        msg.setTopic(this.defaultMQPushConsumer.withNamespace(msg.getTopic()));
        try {
            // 发送消息
            this.defaultMQPushConsumerImpl.sendMessageBack(msg, delayLevel, context.getMessageQueue().getBrokerName());
            return true;
        } catch (Exception e) {
            log.error("sendMessageBack exception, group: " + this.consumerGroup + " msg: " + msg.toString(), e);
        }

        return false;
    }

    private void submitConsumeRequestLater(
        final List<MessageExt> msgs,
        final ProcessQueue processQueue,
        final MessageQueue messageQueue
    ) {

        this.scheduledExecutorService.schedule(new Runnable() {

            @Override
            public void run() {
                ConsumeMessageConcurrentlyService.this.submitConsumeRequest(msgs, processQueue, messageQueue, true);
            }
        }, 5000, TimeUnit.MILLISECONDS);
    }

    /**
     * 这个函数很好玩，是将一个函数放到线程池中推迟执行，那需要推迟执行的函数是什么呢？
     * 是将一个consumeRequest提交到线程池中，所以该函数涉及两个线程池，使用一个定时线程池来延迟将一个runnable提交到线程池中
     */
    private void submitConsumeRequestLater(final ConsumeRequest consumeRequest
    ) {

        this.scheduledExecutorService.schedule(new Runnable() {

            @Override
            public void run() {
                ConsumeMessageConcurrentlyService.this.consumeExecutor.submit(consumeRequest);
            }
        }, 5000, TimeUnit.MILLISECONDS);
    }

    /**
     * 一批消息，在这个线程中进行消费
     */
    class ConsumeRequest implements Runnable {
        private final List<MessageExt> msgs; // 待消费的多个消息
        private final ProcessQueue processQueue; // 代销费的pq
        private final MessageQueue messageQueue;

        public ConsumeRequest(List<MessageExt> msgs, ProcessQueue processQueue, MessageQueue messageQueue) {
            this.msgs = msgs;
            this.processQueue = processQueue;
            this.messageQueue = messageQueue;
        }

        public List<MessageExt> getMsgs() {
            return msgs;
        }

        public ProcessQueue getProcessQueue() {
            return processQueue;
        }

        @Override
        public void run() {
            // 如果pq被销毁了，直接返回
            if (this.processQueue.isDropped()) {
                log.info("the message queue not be able to consume, because it's dropped. group={} {}", ConsumeMessageConcurrentlyService.this.consumerGroup, this.messageQueue);
                return;
            }

            // ipt 获取listener,这就是用户自己定义的那个了
            MessageListenerConcurrently listener = ConsumeMessageConcurrentlyService.this.messageListener;
            ConsumeConcurrentlyContext context = new ConsumeConcurrentlyContext(messageQueue);
            ConsumeConcurrentlyStatus status = null;
            defaultMQPushConsumerImpl.resetRetryAndNamespace(msgs, defaultMQPushConsumer.getConsumerGroup());

            ConsumeMessageContext consumeMessageContext = null;
            // 是否有钩子，如果有钩子，执行钩子
            if (ConsumeMessageConcurrentlyService.this.defaultMQPushConsumerImpl.hasHook()) {
                consumeMessageContext = new ConsumeMessageContext();
                consumeMessageContext.setNamespace(defaultMQPushConsumer.getNamespace());
                // 设置消费者组
                consumeMessageContext.setConsumerGroup(defaultMQPushConsumer.getConsumerGroup());
                consumeMessageContext.setProps(new HashMap<String, String>());
                consumeMessageContext.setMq(messageQueue);
                consumeMessageContext.setMsgList(msgs);
                consumeMessageContext.setSuccess(false);
                //执行消费前钩子
                ConsumeMessageConcurrentlyService.this.defaultMQPushConsumerImpl.executeHookBefore(consumeMessageContext);
            }

            // 记录开始时间，后面用来计算RT
            long beginTimestamp = System.currentTimeMillis();
            boolean hasException = false;
            ConsumeReturnType returnType = ConsumeReturnType.SUCCESS;
            try {
                if (msgs != null && !msgs.isEmpty()) {
                    for (MessageExt msg : msgs) {
                        // 在每个msg中设置消费开始时间
                        MessageAccessor.setConsumeStartTimeStamp(msg, String.valueOf(System.currentTimeMillis()));
                    }
                }
                // ipt 调用用户定义的listener进行消费
                status = listener.consumeMessage(Collections.unmodifiableList(msgs), context);
            } catch (Throwable e) {
                // ipt 注意，这里把用户定义的consumeMessage异常全部做了fail-safe了
                log.warn(String.format("consumeMessage exception: %s Group: %s Msgs: %s MQ: %s",
                    RemotingHelper.exceptionSimpleDesc(e),
                    ConsumeMessageConcurrentlyService.this.consumerGroup,
                    msgs,
                    messageQueue), e);
                hasException = true;
            }
            // 消费的一个RT
            long consumeRT = System.currentTimeMillis() - beginTimestamp;
            if (null == status) { // 没有返回结果有两种可能性，一种是发生了异常，一种是代码里面就是返回null
                if (hasException) {//有异常
                    returnType = ConsumeReturnType.EXCEPTION;
                } else { // 返回null
                    returnType = ConsumeReturnType.RETURNNULL;
                }
            } else if (consumeRT >= defaultMQPushConsumer.getConsumeTimeout() * 60 * 1000) {
                // 响应时间>=15分钟 默认是消费超时时间是15分钟，返回time_out。 注意，这里也只是consumer消费完成后返回time_out
                returnType = ConsumeReturnType.TIME_OUT;
            } else if (ConsumeConcurrentlyStatus.RECONSUME_LATER == status) {
                // 失败
                returnType = ConsumeReturnType.FAILED;
            } else if (ConsumeConcurrentlyStatus.CONSUME_SUCCESS == status) {
                // 成功
                returnType = ConsumeReturnType.SUCCESS;
            }

            if (ConsumeMessageConcurrentlyService.this.defaultMQPushConsumerImpl.hasHook()) {
                consumeMessageContext.getProps().put(MixAll.CONSUME_CONTEXT_TYPE, returnType.name());
            }

            // status到还是null的话, 就设置稍后重新消费
            // status就两种可能性，成功和稍后重试
            if (null == status) {
                log.warn("consumeMessage return null, Group: {} Msgs: {} MQ: {}",
                    ConsumeMessageConcurrentlyService.this.consumerGroup,
                    msgs,
                    messageQueue);
                // 设置稍后重新消费
                status = ConsumeConcurrentlyStatus.RECONSUME_LATER;
            }

            // 执行钩子
            if (ConsumeMessageConcurrentlyService.this.defaultMQPushConsumerImpl.hasHook()) {
                // 设置消费是否成功
                consumeMessageContext.setStatus(status.toString());
                consumeMessageContext.setSuccess(ConsumeConcurrentlyStatus.CONSUME_SUCCESS == status);
                // 执行消费后的钩子
                ConsumeMessageConcurrentlyService.this.defaultMQPushConsumerImpl.executeHookAfter(consumeMessageContext);
            }

            // 记录消费RT
            ConsumeMessageConcurrentlyService.this.getConsumerStatsManager()
                .incConsumeRT(ConsumeMessageConcurrentlyService.this.consumerGroup, messageQueue.getTopic(), consumeRT);

            // 没有删除的话
            if (!processQueue.isDropped()) {
                // 处理消费结果
                ConsumeMessageConcurrentlyService.this.processConsumeResult(status, context, this);
            } else {
                log.warn("processQueue is dropped without process consume result. messageQueue={}, msgs={}", messageQueue, msgs);
            }
        }

        public MessageQueue getMessageQueue() {
            return messageQueue;
        }

    }
}
