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
package org.apache.rocketmq.store.index;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.sysflag.MessageSysFlag;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.store.DispatchRequest;
import org.apache.rocketmq.store.config.StorePathConfigHelper;

public class IndexService {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);
    /**
     * Maximum times to attempt index file creation.
     */
    private static final int MAX_TRY_IDX_CREATE = 3;
    private final DefaultMessageStore defaultMessageStore;
    private final int hashSlotNum;
    private final int indexNum;
    private final String storePath;
    private final ArrayList<IndexFile> indexFileList = new ArrayList<IndexFile>();
    private final ReadWriteLock readWriteLock = new ReentrantReadWriteLock();

    public IndexService(final DefaultMessageStore store) {
        this.defaultMessageStore = store;
        this.hashSlotNum = store.getMessageStoreConfig().getMaxHashSlotNum();
        this.indexNum = store.getMessageStoreConfig().getMaxIndexNum();
        this.storePath =
            StorePathConfigHelper.getStorePathIndex(store.getMessageStoreConfig().getStorePathRootDir());
    }

    public boolean load(final boolean lastExitOK) {
        File dir = new File(this.storePath);
        File[] files = dir.listFiles();
        if (files != null) {
            // ascending order
            Arrays.sort(files);
            for (File file : files) {
                try {
                    IndexFile f = new IndexFile(file.getPath(), this.hashSlotNum, this.indexNum, 0, 0);
                    f.load();

                    if (!lastExitOK) {
                        if (f.getEndTimestamp() > this.defaultMessageStore.getStoreCheckpoint()
                            .getIndexMsgTimestamp()) {
                            f.destroy(0);
                            continue;
                        }
                    }

                    log.info("load index file OK, " + f.getFileName());
                    this.indexFileList.add(f);
                } catch (IOException e) {
                    log.error("load file {} error", file, e);
                    return false;
                } catch (NumberFormatException e) {
                    log.error("load file {} error", file, e);
                }
            }
        }

        return true;
    }

    /**
     * 删除indexFile
     * 可以看到，先是拿第一个indexFile 看看有没有小于commitlog 最小offset 的情况发生，这里也是拿的indexFile最后一个offset 做的对比，
     * 因为这块也是按照offset大小 前后顺序处理的，最后一个的offest 肯定是这个indexFile中最大的了，如果第一个indexFile满足了的话，
     * 就会拿到所有引用，然后遍历找出符合条件的indexFile， 调用deleteExpiredFile方法遍历销毁
     * @param offset commitLog的最小的offset，小于这个offset的都要删除
     */
    public void deleteExpiredFile(long offset) {
        Object[] files = null;
        // 这个try是尝试性的，先拿第一个indexFile看看满不满足
        try {
            // 获取读锁
            this.readWriteLock.readLock().lock();
            if (this.indexFileList.isEmpty()) {
                return;
            }

            // 湖区第一个indexFile的一个offset
            long endPhyOffset = this.indexFileList.get(0).getEndPhyOffset();
            if (endPhyOffset < offset) {
                files = this.indexFileList.toArray();
            }
        } catch (Exception e) {
            log.error("destroy exception", e);
        } finally {
            this.readWriteLock.readLock().unlock();
        }

        if (files != null) {
            // 找出需要删除的indexFile
            List<IndexFile> fileList = new ArrayList<IndexFile>();
            for (int i = 0; i < (files.length - 1); i++) {
                IndexFile f = (IndexFile) files[i];
                if (f.getEndPhyOffset() < offset) {
                    fileList.add(f);
                } else {
                    break;
                }
            }

            this.deleteExpiredFile(fileList);
        }
    }

    private void deleteExpiredFile(List<IndexFile> files) {
        if (!files.isEmpty()) {
            try {
                this.readWriteLock.writeLock().lock();
                for (IndexFile file : files) {
                    // 销毁
                    boolean destroyed = file.destroy(3000);
                    // 从index集合中移除
                    destroyed = destroyed && this.indexFileList.remove(file);
                    if (!destroyed) {
                        log.error("deleteExpiredFile remove failed.");
                        break;
                    }
                }
            } catch (Exception e) {
                log.error("deleteExpiredFile has exception.", e);
            } finally {
                this.readWriteLock.writeLock().unlock();
            }
        }
    }

    public void destroy() {
        try {
            this.readWriteLock.writeLock().lock();
            for (IndexFile f : this.indexFileList) {
                f.destroy(1000 * 3);
            }
            this.indexFileList.clear();
        } catch (Exception e) {
            log.error("destroy exception", e);
        } finally {
            this.readWriteLock.writeLock().unlock();
        }
    }

    public QueryOffsetResult queryOffset(String topic, String key, int maxNum, long begin, long end) {
        List<Long> phyOffsets = new ArrayList<Long>(maxNum);

        long indexLastUpdateTimestamp = 0;
        long indexLastUpdatePhyoffset = 0;
        maxNum = Math.min(maxNum, this.defaultMessageStore.getMessageStoreConfig().getMaxMsgsNumBatch());
        try {
            this.readWriteLock.readLock().lock();
            if (!this.indexFileList.isEmpty()) {
                for (int i = this.indexFileList.size(); i > 0; i--) {
                    IndexFile f = this.indexFileList.get(i - 1);
                    boolean lastFile = i == this.indexFileList.size();
                    if (lastFile) {
                        indexLastUpdateTimestamp = f.getEndTimestamp();
                        indexLastUpdatePhyoffset = f.getEndPhyOffset();
                    }

                    if (f.isTimeMatched(begin, end)) {

                        f.selectPhyOffset(phyOffsets, buildKey(topic, key), maxNum, begin, end, lastFile);
                    }

                    if (f.getBeginTimestamp() < begin) {
                        break;
                    }

                    if (phyOffsets.size() >= maxNum) {
                        break;
                    }
                }
            }
        } catch (Exception e) {
            log.error("queryMsg exception", e);
        } finally {
            this.readWriteLock.readLock().unlock();
        }

        return new QueryOffsetResult(phyOffsets, indexLastUpdateTimestamp, indexLastUpdatePhyoffset);
    }

    private String buildKey(final String topic, final String key) {
        return topic + "#" + key;
    }

    public void buildIndex(DispatchRequest req) {
        // ipt 1、获取或者是创建索引文件
        IndexFile indexFile = retryGetAndCreateIndexFile();
        if (indexFile != null) {
            // 2、获取最后一个物理offset
            long endPhyOffset = indexFile.getEndPhyOffset();
            DispatchRequest msg = req;
            // 3、获取topic
            String topic = msg.getTopic();
            // 4、获取key
            String keys = msg.getKeys();
            // 当msg的offset小于indexFile的最后一个offset，说明这个msg以及落后了
            if (msg.getCommitLogOffset() < endPhyOffset) {
                return;
            }

            // 事务状态
            final int tranType = MessageSysFlag.getTransactionValue(msg.getSysFlag());
            switch (tranType) {
                case MessageSysFlag.TRANSACTION_NOT_TYPE:
                case MessageSysFlag.TRANSACTION_PREPARED_TYPE:
                case MessageSysFlag.TRANSACTION_COMMIT_TYPE:
                    break;
                case MessageSysFlag.TRANSACTION_ROLLBACK_TYPE:
                    // 回滚的不用创建索引了
                    return;
            }

            if (req.getUniqKey() != null) {
                // ipt 这个是构建topic && uniqKey (maybe is msgId)的一个index
                // 我们在塞入keys的时候如果是一个key的话不要有空字符，不然的话它就会当成2个key，然后进行分割
                indexFile = putKey(indexFile, msg, buildKey(topic, req.getUniqKey()));
                if (indexFile == null) {
                    log.error("putKey error commitlog {} uniqkey {}", req.getCommitLogOffset(), req.getUniqKey());
                    return;
                }
            }

            // do
            if (keys != null && keys.length() > 0) {
                String[] keyset = keys.split(MessageConst.KEY_SEPARATOR);
                for (int i = 0; i < keyset.length; i++) {
                    String key = keyset[i];
                    if (key.length() > 0) {
                        // 这个是构建topic && key 的一个index
                        indexFile = putKey(indexFile, msg, buildKey(topic, key));
                        if (indexFile == null) {
                            log.error("putKey error commitlog {} uniqkey {}", req.getCommitLogOffset(), req.getUniqKey());
                            return;
                        }
                    }
                }
            }
        } else {
            log.error("build index error, stop building index");
        }
    }

    // 就是将key放到索引文件中
    private IndexFile putKey(IndexFile indexFile, DispatchRequest msg, String idxKey) {
        // 不断重试，失败了就重新获取一个indexFile(也可能是重新创建一个)
        // ipt 调用indexFile.putKey
        for (boolean ok = indexFile.putKey(idxKey, msg.getCommitLogOffset(), msg.getStoreTimestamp()); !ok; ) {
            log.warn("Index file [" + indexFile.getFileName() + "] is full, trying to create another one");
            // ipt 重新获取或者是创建一个indexFile
            indexFile = retryGetAndCreateIndexFile();
            if (null == indexFile) {
                return null;
            }
            // ipt 继续创建索引
            ok = indexFile.putKey(idxKey, msg.getCommitLogOffset(), msg.getStoreTimestamp());
        }

        return indexFile;
    }

    /**
     * Retries to get or create index file.
     * 带有重试功能的获取或者是创建索引文件
     *
     * @return {@link IndexFile} or null on failure.
     */
    public IndexFile retryGetAndCreateIndexFile() {
        IndexFile indexFile = null;

        // 重试三次，调用getAndCreateLastIndexFile()获取索引文件
        for (int times = 0; null == indexFile && times < MAX_TRY_IDX_CREATE; times++) {
            // ipt 获取索引文件
            indexFile = this.getAndCreateLastIndexFile();
            if (null != indexFile)
                break;

            try {
                log.info("Tried to create index file " + times + " times");
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                log.error("Interrupted", e);
            }
        }

        // 没有获取到这个indexFile的时候
        if (null == indexFile) {
            this.defaultMessageStore.getAccessRights().makeIndexFileError();
            log.error("Mark index file cannot build flag");
        }

        return indexFile;
    }

    public IndexFile getAndCreateLastIndexFile() {
        IndexFile indexFile = null;
        IndexFile prevIndexFile = null;
        long lastUpdateEndPhyOffset = 0;
        long lastUpdateIndexTimestamp = 0;

        {
            // 获取读锁
            this.readWriteLock.readLock().lock();
            if (!this.indexFileList.isEmpty()) {
                // ipt 获取最后一个IndexFile
                IndexFile tmp = this.indexFileList.get(this.indexFileList.size() - 1);
                if (!tmp.isWriteFull()) {
                    // 如果没有写满的话
                    indexFile = tmp;
                } else {
                    // 写满了，indexFile==null
                    lastUpdateEndPhyOffset = tmp.getEndPhyOffset();   // 创建下一个indexFile需要
                    lastUpdateIndexTimestamp = tmp.getEndTimestamp(); // 创建下一个indexFile需要
                    prevIndexFile = tmp; // 保存上一个indexFile，为了后面刷盘
                }
            }

            this.readWriteLock.readLock().unlock();
        }

        // 写满了或者indexFileList是空的，则重新创建indexFile，并将上一个indexFile刷盘
        if (indexFile == null) {
            try {
                // 先搞个文件名出来，年月日时分秒毫秒
                String fileName =
                    this.storePath + File.separator
                        + UtilAll.timeMillisToHumanString(System.currentTimeMillis());
                // 创建一个indexFile
                indexFile =
                    new IndexFile(fileName, this.hashSlotNum, this.indexNum, lastUpdateEndPhyOffset,
                        lastUpdateIndexTimestamp);
                // 获得写锁，将这个indexFile放入集合中
                this.readWriteLock.writeLock().lock();
                this.indexFileList.add(indexFile);
            } catch (Exception e) {
                log.error("getLastIndexFile exception ", e);
            } finally {
                this.readWriteLock.writeLock().unlock();
            }

            // 开启了一个刷盘线程
            if (indexFile != null) {
                final IndexFile flushThisFile = prevIndexFile;
                Thread flushThread = new Thread(new Runnable() {
                    @Override
                    public void run() {
                        // 把上一个indexFile刷盘了
                        IndexService.this.flush(flushThisFile);
                    }
                }, "FlushIndexFileThread");

                flushThread.setDaemon(true);
                flushThread.start();
            }
        }

        return indexFile;
    }

    public void flush(final IndexFile f) {
        if (null == f)
            return;

        long indexMsgTimestamp = 0;

        if (f.isWriteFull()) {
            indexMsgTimestamp = f.getEndTimestamp();
        }

        f.flush();

        if (indexMsgTimestamp > 0) {
            this.defaultMessageStore.getStoreCheckpoint().setIndexMsgTimestamp(indexMsgTimestamp);
            this.defaultMessageStore.getStoreCheckpoint().flush();
        }
    }

    public void start() {

    }

    public void shutdown() {

    }
}
