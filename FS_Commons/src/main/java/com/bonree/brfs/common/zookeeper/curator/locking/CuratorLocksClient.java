package com.bonree.brfs.common.zookeeper.curator.locking;

import java.util.concurrent.TimeUnit;

import org.apache.curator.framework.recipes.locks.InterProcessMutex;

import com.bonree.brfs.common.zookeeper.curator.CuratorZookeeperClient;

/*******************************************************************************
 * 版权信息：博睿宏远科技发展有限公司
 * Copyright: Copyright (c) 2007博睿宏远科技发展有限公司,Inc.All Rights Reserved.
 * 
 * @date 2018年3月12日 下午6:39:42
 * @Author: <a href=mailto:weizheng@bonree.com>魏征</a>
 * @Description: 
 ******************************************************************************/
public class CuratorLocksClient {

    private Executor instance;
    private final InterProcessMutex lock;
    private final String lockName;
    private final String lockPath;
    private final CuratorZookeeperClient client;

    public CuratorLocksClient(CuratorZookeeperClient client, String lockPath, Executor executor, String lockName) {
        this.instance = executor;
        this.lockName = lockName;
        this.client = client;
        this.lockPath = lockPath;
        lock = new InterProcessMutex(client.getInnerClient(), lockPath);
    }

    public void doWork(long time, TimeUnit unit) throws Exception {
        if (!lock.acquire(time, unit)) {
            throw new IllegalStateException(lockName + " could not acquire the lock");
        }
        try {
            System.out.println(client.getChildren(lockPath));
            instance.execute(client);
        } finally {
            lock.release(); // always release the lock in a finally block
        }
    }

    public void doWork() throws Exception {
        lock.acquire();
        try {
            instance.execute(client);
        } finally {
            lock.release(); // always release the lock in a finally block
        }
    }

    public String getLockName() {
        return lockName;
    }

}
