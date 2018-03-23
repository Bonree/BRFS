package com.bonree.brfs.common.zookeeper.curator.locking;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.curator.framework.recipes.locks.InterProcessLock;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.jboss.netty.util.internal.ConcurrentHashMap;

import com.bonree.brfs.common.zookeeper.curator.CuratorClient;

/*******************************************************************************
 * 版权信息：博睿宏远科技发展有限公司
 * Copyright: Copyright (c) 2007博睿宏远科技发展有限公司,Inc.All Rights Reserved.
 * 
 * @date 2018年3月12日 下午6:39:42
 * @Author: <a href=mailto:weizheng@bonree.com>魏征</a>
 * @Description: 封装的curator lock类
 ******************************************************************************/
public class CuratorLocksClient<T> {

    private Executor<T> instance;
    private final InterProcessLock lock;
    private final String lockName;
    private final String lockPath;
    private final CuratorClient client;
    
    private final Map<String,InterProcessLock> locksMap;

    public CuratorLocksClient(CuratorClient client, String lockPath, Executor<T> executor, String lockName) {
        this.instance = executor;
        this.lockName = lockName;
        this.client = client;
        this.lockPath = lockPath;
        locksMap = new ConcurrentHashMap<>();
        lock = new InterProcessMutex(client.getInnerClient(), lockPath);
    }

    /** 概述：
     * @param time
     * @param unit
     * @throws Exception
     * @user <a href=mailto:weizheng@bonree.com>魏征</a>
     */
    public T execute(long time, TimeUnit unit) throws Exception {
        if (!lock.acquire(time, unit)) {
            throw new IllegalStateException(lockName + " could not acquire the lock");
        }
        try {
            return instance.execute(client);
        } finally {
            lock.release();
        }
    }

    public T execute() throws Exception {
        lock.acquire();
        try {
            return instance.execute(client);
        } finally {
            lock.release();
        }
    }

    public String getLockName() {
        return lockName;
    }

    public String getLockPath() {
        return lockPath;
    }

}
