package com.bonree.brfs.duplication.filenode.zk;

import com.bonree.brfs.common.ZookeeperPaths;
import com.bonree.brfs.common.lifecycle.LifecycleStart;
import com.bonree.brfs.common.lifecycle.LifecycleStop;
import com.bonree.brfs.common.lifecycle.ManageLifecycle;
import com.bonree.brfs.common.service.Service;
import com.bonree.brfs.common.service.ServiceManager;
import com.bonree.brfs.common.timer.TimeExchangeEventEmitter;
import com.bonree.brfs.common.utils.CloseUtils;
import com.bonree.brfs.common.utils.JsonUtils;
import com.bonree.brfs.duplication.datastream.file.FileObjectCloser;
import com.bonree.brfs.duplication.filenode.FileNode;
import com.bonree.brfs.duplication.filenode.FileNodeSink;
import com.bonree.brfs.duplication.filenode.FileNodeSinkManager;
import com.bonree.brfs.duplication.filenode.FileNodeSinkSelector;
import com.bonree.brfs.duplication.filenode.FileNodeStorer;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.inject.Inject;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.framework.recipes.leader.LeaderSelector;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListener;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.utils.ZKPaths;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ManageLifecycle
public class ZkFileNodeSinkManager implements FileNodeSinkManager {
    private static final Logger LOG = LoggerFactory.getLogger(ZkFileNodeSinkManager.class);

    private final Service service;

    private CuratorFramework client;
    private LeaderSelector selector;

    private Map<String, PathChildrenCache> fileNodeSinks = new HashMap<String, PathChildrenCache>();

    private AtomicBoolean isLeader = new AtomicBoolean(false);

    private FileNodeDistributor distributor;

    private CopyOnWriteArrayList<StateListener> stateListeners = new CopyOnWriteArrayList<FileNodeSinkManager.StateListener>();

    @Inject
    public ZkFileNodeSinkManager(
        CuratorFramework client,
        ZookeeperPaths paths,
        Service service,
        ServiceManager serviceManager,
        TimeExchangeEventEmitter timeEventEmitter,
        FileNodeStorer storer,
        FileNodeSinkSelector selector,
        FileObjectCloser fileCloser) {
        this.client = client.usingNamespace(paths.getBaseClusterName().substring(1));
        this.service = service;
        this.distributor = new FileNodeDistributor(this.client, storer, serviceManager, selector, timeEventEmitter, fileCloser);
        this.selector = new LeaderSelector(this.client, ZKPaths.makePath(
            ZkFileCoordinatorPaths.COORDINATOR_ROOT, ZkFileCoordinatorPaths.COORDINATOR_LEADER),
                                           new SinkManagerLeaderListener());
    }

    @LifecycleStart
    public void start() throws Exception {
        selector.autoRequeue();
        selector.start();
    }

    @LifecycleStop
    public void stop() throws Exception {
        selector.close();
    }

    @Override
    public void registerFileNodeSink(FileNodeSink sink) {
        //PathChildrenCache会自动创建sink节点所在的路径
        PathChildrenCache sinkWatcher =
            new PathChildrenCache(client,
                                  ZkFileCoordinatorPaths.buildSinkPath(service, sink.getStorageRegion().getName()),
                                  true);
        sinkWatcher.getListenable().addListener(new SinkNodeListener(sink));

        try {
            sinkWatcher.start();
            fileNodeSinks.put(sink.getStorageRegion().getName(), sinkWatcher);
        } catch (Exception e) {
            LOG.error("register file node sink for region[{}] error!", sink.getStorageRegion().getName(), e);
        }
    }

    @Override
    public void unregisterFileNodeSink(FileNodeSink sink) {
        PathChildrenCache sinkWatcher = fileNodeSinks.get(sink.getStorageRegion().getName());
        if (sinkWatcher != null) {
            try {
                sinkWatcher.close();
                client.delete().quietly().deletingChildrenIfNeeded()
                      .forPath(ZkFileCoordinatorPaths.buildSinkPath(service, sink.getStorageRegion().getName()));
            } catch (Exception e) {
                LOG.error("unregister file node sink for region[{}] error!", sink.getStorageRegion().getName(), e);
            }
        }
    }

    @Override
    public void addStateListener(StateListener listener) {
        this.stateListeners.add(listener);
    }

    @Override
    public void removeStateListener(StateListener listener) {
        this.stateListeners.remove(listener);
    }

    /**
     * Leader选举结果监听类
     *
     * @author yupeng
     */
    private class SinkManagerLeaderListener implements LeaderSelectorListener {

        @Override
        public void stateChanged(CuratorFramework client, ConnectionState newState) {
            if (!newState.isConnected()) {
                synchronized (isLeader) {
                    CloseUtils.closeQuietly(distributor);
                    isLeader.set(false);
                    isLeader.notifyAll();
                }
            }

            for (StateListener listener : stateListeners) {
                try {
                    listener.stateChanged(newState.isConnected());
                } catch (Exception e) {
                    LOG.error("notify state listener error!", e);
                }
            }
        }

        @Override
        public void takeLeadership(CuratorFramework client) throws Exception {
            LOG.info("I am leader!");
            isLeader.set(true);

            try {
                distributor.start();

                synchronized (isLeader) {
                    if (isLeader.get()) {
                        isLeader.wait();
                    }
                }
            } catch (InterruptedException e) {
                LOG.warn("Leader relationship is terminated by interrupt event!");
            } finally {
                isLeader.set(false);
                distributor.close();
            }
        }

    }

    /**
     * Sink中文件节点变化情况监听类
     *
     * @author yupeng
     */
    private class SinkNodeListener implements PathChildrenCacheListener {
        private FileNodeSink sink;

        public SinkNodeListener(FileNodeSink sink) {
            this.sink = sink;
        }

        @Override
        public void childEvent(CuratorFramework client,
                               PathChildrenCacheEvent event) throws Exception {
            ChildData data = event.getData();
            if (data == null) {
                return;
            }

            LOG.info("EVENT--{}--{}", event.getType(), data.getPath());
            switch (event.getType()) {
            case CHILD_ADDED:
                FileNode fileNode = JsonUtils.toObject(data.getData(), FileNode.class);
                sink.received(fileNode);
                //如果节点接受成功则删除sink中的节点
                client.delete().quietly().forPath(data.getPath());
                break;
            default:
                break;
            }
        }

    }

}
