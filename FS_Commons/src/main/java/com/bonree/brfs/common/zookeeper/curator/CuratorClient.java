package com.bonree.brfs.common.zookeeper.curator;

import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.CuratorListener;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.curator.retry.RetryNTimes;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.Watcher;

import com.bonree.brfs.common.zookeeper.StateListener;
import com.bonree.brfs.common.zookeeper.ZookeeperClient;

/*******************************************************************************
 * 版权信息：博睿宏远科技发展有限公司
 * Copyright: Copyright (c) 2007博睿宏远科技发展有限公司,Inc.All Rights Reserved.
 * 
 * @date 2018年3月12日 下午1:53:06
 * @Author: <a href=mailto:weizheng@bonree.com>魏征</a>
 * @Description: client helper
 ******************************************************************************/
public class CuratorClient implements ZookeeperClient {

    private final CuratorFramework client;

    private volatile StateListener stateListeners = StateListener.DISCONNECTED;

    private final static RetryPolicy RETRY_POLICY = new RetryNTimes(1, 1000);

    private final static int SESSION_TIMEOUT_MS = 300 * 1000;

    private final static int CONNECTION_TIMEOUT_MS = 300 * 1000;

    private final static String DEFAULT_VALUE = "";

    public static CuratorClient wrapClient(CuratorFramework client) {
        if(!client.isStarted()) {
            client.start();
        }
        CuratorClient curatorClient = new CuratorClient(client);
        return curatorClient;
    }

    public static CuratorClient getClientInstance(String zkHosts) {
        return new CuratorClient(zkHosts, RETRY_POLICY, SESSION_TIMEOUT_MS, CONNECTION_TIMEOUT_MS);
    }

    public static CuratorClient getClientInstance(String zkHosts, RetryPolicy retry) {
        return new CuratorClient(zkHosts, retry, SESSION_TIMEOUT_MS, CONNECTION_TIMEOUT_MS);
    }

    public static CuratorClient getClientInstance(String zkHosts, RetryPolicy retry, int sessionTimeoutMs) {
        return new CuratorClient(zkHosts, retry, sessionTimeoutMs, CONNECTION_TIMEOUT_MS);
    }

    public static CuratorClient getClientInstance(String zkHosts, RetryPolicy retry, int sessionTimeoutMs, int connectionTimeoutMs) {
        return new CuratorClient(zkHosts, retry, sessionTimeoutMs, connectionTimeoutMs);
    }

    public static CuratorClient getClientInstance(String zkHosts, int sessionTimeoutMs, int connectionTimeoutMs) {
        return new CuratorClient(zkHosts, RETRY_POLICY, sessionTimeoutMs, connectionTimeoutMs);
    }

    public CuratorClient(CuratorFramework client) {
        this.client = client;
    }

    public CuratorClient(String zkHosts, RetryPolicy retry, int sessionTimeoutMs, int connectionTimeoutMs) {
        try {
            CuratorFrameworkFactory.Builder builder = CuratorFrameworkFactory.builder().connectString(zkHosts).retryPolicy(retry).connectionTimeoutMs(connectionTimeoutMs).sessionTimeoutMs(sessionTimeoutMs);
           
            client = builder.build();
            client.getConnectionStateListenable().addListener(new ConnectionStateListener() {

                @Override
                public void stateChanged(CuratorFramework client, ConnectionState newState) {
                    if (newState == ConnectionState.LOST) {
                        stateListeners = StateListener.DISCONNECTED;
                    } else if (newState == ConnectionState.CONNECTED) {
                        stateListeners = StateListener.CONNECTED;
                    } else if (newState == ConnectionState.RECONNECTED) {
                        stateListeners = StateListener.RECONNECTED;
                    } else if (newState == ConnectionState.SUSPENDED) {
                        stateListeners = StateListener.SUSPENDED;
                    }
                }
            });
            client.start();
        } catch (Exception e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    public void blockUntilConnected() throws InterruptedException {
        client.blockUntilConnected();
    }

    public void blockUntilConnected(int maxWaitTime, TimeUnit units) throws InterruptedException {
        client.blockUntilConnected(maxWaitTime, units);
    }

    public CuratorFramework getInnerClient() {
        return client;
    }

    @Override
    public void delete(String path, boolean isRecursion) {
        try {
            if (isRecursion) {
                client.delete().deletingChildrenIfNeeded().forPath(path);
            } else {
                client.delete().forPath(path);
            }
        } catch (Exception e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }
    @Override
    public void checkAndDelte(String path, boolean isRecursion) {
        if(checkExists(path)) {
            delete(path,isRecursion);
        }
        
    }

    @Override
    public List<String> getChildren(String path) {
        try {
            return client.getChildren().forPath(path);
        } catch (NoNodeException e) {
            return null;
        } catch (Exception e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    public StateListener getStateListener() {
        return stateListeners;
    }

    @Override
    public boolean isConnected() {
        return client.getZookeeperClient().isConnected();
    }

    @Override
    public void useNameSpace(String path) {
        CuratorFramework tmpClient = client.usingNamespace(path);
        wrapClient(tmpClient);
    }

    @Override
    public void close() {
        if (client != null) {
            client.close();
        }
    }

    @Override
    public String createPersistent(String path, boolean isRecursion) {
        return createPersistent(path, isRecursion, DEFAULT_VALUE.getBytes());
    }

    @Override
    public String createEphemeral(String path, boolean isRecursion) {
        return createEphemeral(path, isRecursion, DEFAULT_VALUE.getBytes());
    }

    @Override
    public boolean checkExists(String path) {
        try {
            if (client.checkExists().forPath(path) != null) {
                return true;
            }
        } catch (Exception e) {

        }
        return false;
    }

    @Override
    public String createPersistent(String path, boolean isRecursion, byte[] data) {
        try {
            if (isRecursion) {
                return client.create().creatingParentsIfNeeded().forPath(path, data);
            } else {
                return client.create().forPath(path, data);
            }
        } catch (Exception e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    @Override
    public String createPersistentSequential(String path, boolean isRecursion) {
        return createPersistentSequential(path, isRecursion, DEFAULT_VALUE.getBytes());

    }

    @Override
    public String createPersistentSequential(String path, boolean isRecursion, byte[] data) {
        try {
            if (isRecursion) {
                return client.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT_SEQUENTIAL).forPath(path, data);
            } else {
                return client.create().withMode(CreateMode.PERSISTENT_SEQUENTIAL).forPath(path, data);
            }
        } catch (Exception e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    @Override
    public String createEphemeral(String path, boolean isRecursion, byte[] data) {
        try {
            if (isRecursion) {
                return client.create().creatingParentContainersIfNeeded().withMode(CreateMode.EPHEMERAL).forPath(path, data);
            } else {
                return client.create().withMode(CreateMode.EPHEMERAL).forPath(path, data);
            }
        } catch (Exception e) {
            throw new IllegalStateException(e.getMessage(), e);
        }

    }

    @Override
    public String createEphemeralSequential(String path, boolean isRecursion) {
        return createEphemeralSequential(path, isRecursion, DEFAULT_VALUE.getBytes());

    }

    @Override
    public String createEphemeralSequential(String path, boolean isRecursion, byte[] data) {
        try {
            if (isRecursion) {
                return client.create().creatingParentContainersIfNeeded().withProtection().withMode(CreateMode.EPHEMERAL_SEQUENTIAL).forPath(path, data);
            } else {
                return client.create().withProtection().withMode(CreateMode.EPHEMERAL_SEQUENTIAL).forPath(path, data);
            }
        } catch (Exception e) {
            throw new IllegalStateException(e.getMessage(), e);
        }

    }

    @Override
    public void setData(String path, byte[] data) {
        try {
            client.setData().forPath(path, data);
        } catch (Exception e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    @Override
    public void setDataAsync(String path, byte[] data, CuratorListener listener) {
        client.getCuratorListenable().addListener(listener);

        try {
            client.setData().inBackground().forPath(path, data);
        } catch (Exception e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    @Override
    public void guaranteedDelete(String path, boolean isRecursion) {
        try {
            if (isRecursion) {
                client.delete().guaranteed().deletingChildrenIfNeeded().forPath(path);
            } else {
                client.delete().guaranteed().forPath(path);
            }
        } catch (Exception e) {
            throw new IllegalStateException(e.getMessage(), e);
        }

    }

    @Override
    public List<String> watchedGetChildren(String path) {

        try {
            return client.getChildren().watched().forPath(path);
        } catch (Exception e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    @Override
    public List<String> watchedGetChildren(String path, Watcher watcher) {
        try {
            return client.getChildren().usingWatcher(watcher).forPath(path);
        } catch (Exception e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    @Override
    public byte[] getData(String path) {
        try {
            return client.getData().forPath(path);
        } catch (Exception e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

}
