package com.bonree.brfs.metadata.backup;

import com.bonree.brfs.metadata.ZNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/*******************************************************************************
 * 版权信息：北京博睿宏远数据科技股份有限公司
 * Copyright: Copyright (c) 2007博睿宏远科技发展有限公司,Inc.All Rights Reserved.
 *
 * @Date 2020/4/16 14:31
 * @Author: <a href=mailto:zhangqi@bonree.com>张奇</a>
 * @Description: 默认元数据备份引擎
 ******************************************************************************/
public class DefaultMetadataBackupEngine implements MetadataBackupEngine{

    private static final Logger LOG = LoggerFactory.getLogger(DefaultMetadataBackupEngine.class);

    private final int threadsNumber;
    private String zkHost;
    private String zkPath;
    private int timeout;

    private static final int DEFAULT_TIMEOUT = 40000;

    public DefaultMetadataBackupEngine(String zkHost, String zkPath, int threads) {
        this.threadsNumber = threads;
        this.zkHost = zkHost;
        this.zkPath = zkPath;
        this.timeout = DEFAULT_TIMEOUT;
    }

    /**
     * Create new reader instance for a given source.
     *
     * @param zkHost  address of the data to read
     * @param zkPath  zkPath of the data to read
     * @param threads number of concurrent thread for reading data
     * @param timeout the session timeout for read operations
     */
    public DefaultMetadataBackupEngine(String zkHost, String zkPath, int threads, int timeout) {
        this.threadsNumber = threads;
        this.zkHost = zkHost;
        this.zkPath = zkPath;
        this.timeout = timeout;
    }


    /**
     * Read data from the source.
     */
    @Override
    public ZNode backup() {
        LOG.info("Reading [{}] from [{}]", zkPath, zkHost);

        ZNode znode = new ZNode(zkPath);

        ReaderThreadFactory threadFactory = new ReaderThreadFactory(zkHost, timeout);
        ExecutorService pool = Executors.newFixedThreadPool(threadsNumber, threadFactory);

        AtomicInteger totalCounter = new AtomicInteger(0);
        AtomicInteger processedCounter = new AtomicInteger(0);
        AtomicBoolean failed = new AtomicBoolean(false);

        pool.execute(new ZNodeReader(pool, znode, totalCounter, processedCounter, failed));

        try {
            while (true) {
                if (pool.awaitTermination(1, TimeUnit.SECONDS)) {
                    LOG.info("Metadata backup completed.");
                    break;
                }
                LOG.info("Processing, total = {}, processed = {}", totalCounter, processedCounter);
                if (totalCounter.get() == processedCounter.get()) {
                    // all work finished
                    pool.shutdown();
                }
            }
        } catch (InterruptedException e) {
            LOG.error("Await Termination of pool was unsuccessful", e);
            return null;
        } finally {
            threadFactory.closeZookeepers();
        }

        if (failed.get()) {
            return null;
        }
        return znode;
    }

}
