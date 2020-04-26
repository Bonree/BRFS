package com.bonree.brfs.metadata.backup;

import com.bonree.brfs.metadata.ZNode;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*******************************************************************************
 * 版权信息：北京博睿宏远数据科技股份有限公司
 * Copyright: Copyright (c) 2007博睿宏远科技发展有限公司,Inc.All Rights Reserved.
 *
 * @Date 2020/4/16 15:04
 * @Author: <a href=mailto:zhangqi@bonree.com>张奇</a>
 * @Description: 读取一个ZNode的信息
 ******************************************************************************/
public class ZNodeReader implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(ZNodeReader.class);

    private final ZNode znode;
    private final ExecutorService pool;
    private final AtomicInteger totalCounter;
    private final AtomicInteger processedCounter;

    private final AtomicBoolean failed;

    ZNodeReader(ExecutorService pool, ZNode znode, AtomicInteger totalCounter, AtomicInteger processedCounter,
                AtomicBoolean failed) {
        this.znode = znode;
        this.pool = pool;
        this.totalCounter = totalCounter;
        this.processedCounter = processedCounter;
        this.failed = failed;
        totalCounter.incrementAndGet();
    }

    @Override
    public void run() {
        try {
            if (failed.get()) {
                return;
            }

            ReaderThread thread = (ReaderThread) Thread.currentThread();
            ZooKeeper zk = thread.getZooKeeper();
            Stat stat = new Stat();
            String path = znode.getAbsolutePath();
            LOG.debug("Reading node [{}]", path);

            byte[] data = zk.getData(path, false, stat);
            if (stat.getEphemeralOwner() != 0) {
                znode.setEphemeral(true);
            }
            znode.setData(data);
            znode.setMtime(stat.getMtime());

            List<String> children = zk.getChildren(path, false);
            for (String child : children) {
                if ("zookeeper".equals(child)) {
                    // reserved
                    continue;
                }
                ZNode node = new ZNode(znode, child);
                znode.appendChild(node);
                pool.execute(new ZNodeReader(pool, node, totalCounter, processedCounter, failed));
            }
        } catch (KeeperException | InterruptedException e) {
            LOG.error("Could not read from remote server", e);
            failed.set(true);
        } finally {
            processedCounter.incrementAndGet();
        }
    }
}
