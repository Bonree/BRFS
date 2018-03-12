package com.bonree.brfs.zookeeper.curator.locking;

import java.util.concurrent.TimeUnit;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;

/*******************************************************************************
 * 版权信息：博睿宏远科技发展有限公司
 * Copyright: Copyright (c) 2007博睿宏远科技发展有限公司,Inc.All Rights Reserved.
 * 
 * @date 2018年3月12日 下午6:39:42
 * @Author: <a href=mailto:weizheng@bonree.com>魏征</a>
 * @Description: 
 ******************************************************************************/
public class CuratorLocks {

    private Executor instance;
    private final InterProcessMutex lock;
    private final String lockName;

    public CuratorLocks(CuratorFramework client, String lockPath, Executor executor, String lockName) {
        this.instance = executor;
        this.lockName = lockName;
        lock = new InterProcessMutex(client, lockPath);
    }

    public void doWork(long time, TimeUnit unit) throws Exception {
        if (!lock.acquire(time, unit)) {
            throw new IllegalStateException(lockName + " could not acquire the lock");
        }
        try {
            System.out.println(lockName + " has the lock");
            instance.execute();
        } finally {
            System.out.println(lockName + " releasing the lock");
            lock.release(); // always release the lock in a finally block
        }
    }
    
    public void doWork() throws Exception {
        lock.acquire();
        try {
            System.out.println(lockName + " has the lock");
            instance.execute();
        } finally {
            System.out.println(lockName + " releasing the lock");
            lock.release(); // always release the lock in a finally block
        }
    }

    public String getLockName() {
        return lockName;
    }

}
