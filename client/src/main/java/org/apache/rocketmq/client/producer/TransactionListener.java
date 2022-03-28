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
package org.apache.rocketmq.client.producer;

import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;

/**
 * 我们在使用RocketMQ做事务消息的时候，需要先发送我们的事务消息，然后当我们事务消息发送成功之后，在执行本地事务，本地事务执行成功，就需要提交事务消息，如果失败，就回滚事务消息。
 * 下面我们就详细介绍下这个流程
 * 我们就那支付成功，发送用户积分这个案例来说吧，我们有这么一个场景，假设我们在做一个电商项目， 然后当用户下单并支付成功的时候，会根据用户支付的金额来计算并向用户放一定的积分，
 * 我们这个发放积分动作不能拖累我们的支付动作，不能因为这种枝叶业务影响我们支付主业务的体验， 所以我们将发放积分的需求使用RocketMQ的事务消息来完成。
 *
 * 一个用户下了单，然后这个时候到了支付阶段了，那么我们先根据用户提交过来的订单，计算好要给用户发放的积分，封装Message，进行消息发送，我们将本地事务流程放到
 * 我们自己实现的listener的executeLocalTransaction 方法,当事务消息发送到RocketMQ的时候，然后它就会调用executeLocalTransaction 方法来执行本地事务，
 * 也就是支付流程，如果用户支付成功，并写入支付记录，修改了订单状态 提交事务之后 执行 return LocalTransactionState.COMMIT_MESSAGE 提交事务消息，
 * 如果支付失败了就return LocalTransactionState.ROLLBACK_MESSAGE 回滚事务消息，这个其实就是告诉RocketMQ这个本地事务成功了或者失败了，需要进行消息的提交或者回滚动作
 * 这里有几个点需要注意下：
 * （1）我在executeLocalTransaction执行本地事务，我需要的参数应该怎么传递，比如需要支付的订单…一堆东西，这个参数传递是在发送消息的时候传递的，也就是第一节那个args 。
 * （2）如果我发送消息失败了怎么处理，发送消息失败就抛出异常，就不用走支付流程了。
 * (3) 如果我提交了本地事务，然后在告诉RocketMQ事务状态的时候，服务挂了，或者网络波动，RocketMQ的broker没有收到我得提交消息怎么办？这个也不用担心，
 * 我们listener 还有一个方法checkLocalTransaction 这个方法就是用来检查我们本地事务的，我们上面那个线程池就是干这个事情的，执行checkLocalTransaction 方法的，
 * 这个方法就是询问你本地事务执行结果是什么样的，这里应该怎样做呢？其实你可以在执行本地事务那个方法里面将msg的一个事务id记录下来，与你这个支付记录关联起来，
 * 到时候RocketMQ来询问你这个本地事务状态的时候，它会将那个msg传过来，msg里面有个事务id，你可以拿着这个事务id去查找你这个支付记录，看看支付状态，
 * 然后告诉RocketMQ是提交事务还是回滚事务。
 *
 * 好了，到这我们就将RocketMQ事务流程解释完成了。
 *
 * 接下来介绍一下原理
 * （1）我们发送这个事务消息的时候一开始并不是直接发送到你指定的那个topic 对应的队列里面的，而是将消息发送到RMQ_SYS_TRANS_HALF_TOPIC topic里面，
 * 然后它返回响应 告诉生产者，执行executeLocalTransaction 方法来执行本地事务，为啥不放了你设置的那个 topic里？就是防止消费者给消费了，
 * 这个时候还不知道你本地事务执行情况，就给消费了岂不是很尴尬。
 * （2）当你本地事务执行成功，返回 commit提交事务，这个时候broker 会 先从RMQ_SYS_TRANS_HALF_TOPIC topic里面找到你那个消息，恢复原来的样子，
 * 然后进行存储，这个时候存储就存储到了 你设置到的那个topic里面了。存储成功之后将 形成一个删除消息 然后放到RMQ_SYS_TRANS_OP_HALF_TOPIC topic 里面。
 * 放到你原来那个topic里面，你的消费者就可以消费了
 * （3）如果你本地事务失败，然后就要rollback 了，这个时候先从RMQ_SYS_TRANS_HALF_TOPIC topic里面找到你那个消息，然后形成一个删除消息
 * 然后放到RMQ_SYS_TRANS_OP_HALF_TOPIC topic 里面。
 * （4）出现网络问题或者服务挂了怎么办？
 * 如果你在发送消息的时候出现了问题，消息是使用同步发送，然后会重试，然后会抛出异常，发送失败，你的本地事务也就不用执行了。
 * 如果你告诉broker 提交事务或者回滚事务的时候出现了问题怎么办？这时候broker 会有个事务服务线程，隔一段时间就扫描RMQ_SYS_TRANS_HALF_TOPIC topic
 * 里面没有提交或者回滚的消息，然后它就会发送消息到你的生产者端来，然后执行checkLocalTransaction 这个检查事务的方法，询问你事务的执行状态。
 * 它默认会问你15次，15次没结果就直接删了，估计是绝望了。
 * ————————————————
 * 版权声明：本文为CSDN博主「$码出未来」的原创文章，遵循CC 4.0 BY-SA版权协议，转载请附上原文出处链接及本声明。
 * 原文链接：https://blog.csdn.net/yuanshangshenghuo/article/details/109389082
 */
public interface TransactionListener {
    /**
     * When send transactional prepare(half) message succeed, this method will be invoked to execute local transaction.
     * 当发送事务准备(half)消息成功时，将调用此方法执行本地事务
     *
     * @param msg Half(prepare) message
     * @param arg Custom business parameter
     * @return Transaction state
     */
    LocalTransactionState executeLocalTransaction(final Message msg, final Object arg);

    /**
     * When no response to prepare(half) message. broker will send check message to check the transaction status, and this
     * method will be invoked to get local transaction status.
     * 当没有回应准备(half)消息，代理将发送检查消息来检查是否状态，并执行此操作
     * 方法将被调用以获取本地事务状态
     *
     * @param msg Check message
     * @return Transaction state
     */
    LocalTransactionState checkLocalTransaction(final MessageExt msg);
}