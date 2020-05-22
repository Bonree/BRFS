package com.bonree.brfs.duplication.filenode.zk;

import com.bonree.brfs.common.service.Service;
import com.bonree.brfs.common.service.ServiceManager;
import com.bonree.brfs.common.service.ServiceStateListener;
import com.bonree.brfs.common.timer.TimeExchangeEventEmitter;
import com.bonree.brfs.common.timer.TimeExchangeListener;
import com.bonree.brfs.common.utils.CloseUtils;
import com.bonree.brfs.common.utils.JsonUtils;
import com.bonree.brfs.common.utils.PooledThreadFactory;
import com.bonree.brfs.common.utils.TimeUtils;
import com.bonree.brfs.configuration.Configs;
import com.bonree.brfs.configuration.units.CommonConfigs;
import com.bonree.brfs.duplication.datastream.file.FileObject;
import com.bonree.brfs.duplication.datastream.file.FileObjectCloser;
import com.bonree.brfs.duplication.filenode.FileNode;
import com.bonree.brfs.duplication.filenode.FileNodeSinkSelector;
import com.bonree.brfs.duplication.filenode.FileNodeStorer;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;
import java.io.Closeable;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 把已消失的Service管理的FileNode分配到其他可用Service中，保证
 * 文件节点可以继续写入
 *
 * @author yupeng
 */
class FileNodeDistributor implements ServiceStateListener, TimeExchangeListener, Closeable {
    private static final Logger LOG = LoggerFactory.getLogger(FileNodeDistributor.class);

    private static final String DUPLICATE_SERVICE_GROUP =
        Configs.getConfiguration().getConfig(CommonConfigs.CONFIG_REGION_SERVICE_GROUP_NAME);

    private Table<String, String, Long> serviceTimeTable = HashBasedTable.create();
    private Map<String, PathChildrenCache> childWatchers = new HashMap<String, PathChildrenCache>();

    private LinkedList<FileNode> wildFileNodes = new LinkedList<FileNode>();

    private FileNodeSinkSelector serviceSelector;

    private CuratorFramework client;
    private FileNodeStorer fileStorer;
    private ServiceManager serviceManager;
    private FileObjectCloser fileCloser;

    private static final String TIMEOUT_CHECK_DURATION = "PT10M";
    private TimeExchangeEventEmitter timeEventEmitter;

    private ExecutorService executor;

    private AtomicBoolean running = new AtomicBoolean(false);

    public FileNodeDistributor(CuratorFramework client, FileNodeStorer fileStorer,
                               ServiceManager serviceManager,
                               FileNodeSinkSelector serviceSelector,
                               TimeExchangeEventEmitter timeEventEmitter,
                               FileObjectCloser fileCloser) {
        this.client = client;
        this.fileStorer = fileStorer;
        this.serviceManager = serviceManager;
        this.serviceSelector = serviceSelector;
        this.timeEventEmitter = timeEventEmitter;
        this.fileCloser = fileCloser;
    }

    public void start() throws Exception {
        if (!running.compareAndSet(false, true)) {
            LOG.error("file distributor has been started!");
            throw new IllegalStateException("file distributor has been started");
        }

        executor = Executors.newSingleThreadExecutor(new PooledThreadFactory("file_distributor"));
        executor.submit(new Runnable() {

            @Override
            public void run() {
                //任务开始前需要先进行文件扫描，确定需要转移的文件
                for (FileNode fileNode : fileStorer.listFileNodes()) {
                    LOG.info("find FileNode[{}], service[{}:{}] create at[{}]",
                             fileNode.getName(),
                             fileNode.getServiceGroup(),
                             fileNode.getServiceId(),
                             new DateTime(fileNode.getCreateTime())
                    );
                    Long serviceTime = serviceTimeTable.get(fileNode.getServiceGroup(), fileNode.getServiceId());
                    if (serviceTime == null) {
                        for (Service service : serviceManager.getServiceListByGroup(fileNode.getServiceGroup())) {
                            serviceTimeTable.put(service.getServiceGroup(), service.getServiceId(), service.getRegisterTime());
                        }

                        serviceTime = serviceTimeTable.get(fileNode.getServiceGroup(), fileNode.getServiceId());
                    }

                    if (serviceTime != null && serviceTime <= fileNode.getServiceTime()) {
                        LOG.info("skip FileNode[{}] with nodeServiceTime[{}] after serviceTime[{}]",
                                 fileNode.getName(),
                                 new DateTime(fileNode.getServiceTime()),
                                 new DateTime(serviceTime)
                        );
                        continue;
                    }

                    if (timeToLive(fileNode) >= 0) {
                        wildFileNodes.add(fileNode);
                        LOG.info("add FileNode[{}] to wild file list", fileNode.getName());
                        continue;
                    }

                    LOG.info("close expired file[{}]", fileNode.getName());
                    fileCloser.close(new FileObject(fileNode), true);
                }

                dispatchWildFileNode();
            }
        });

        serviceManager.addServiceStateListener(DUPLICATE_SERVICE_GROUP, this);
        timeEventEmitter.addListener(TIMEOUT_CHECK_DURATION, this);
    }

    @Override
    public void close() {
        if (running.compareAndSet(true, false)) {
            timeEventEmitter.removeListener(TIMEOUT_CHECK_DURATION, this);
            serviceManager.removeServiceStateListener(DUPLICATE_SERVICE_GROUP, this);

            executor.shutdownNow();
            wildFileNodes.clear();

            for (PathChildrenCache watcher : childWatchers.values()) {
                CloseUtils.closeQuietly(watcher);
            }
            childWatchers.clear();
        }
    }

    //获取可以接受属于特定数据库的文件的服务列表
    private List<Service> getServiceWithStorageRegionName(String storageRegionName) {
        List<Service> result = new ArrayList<Service>();

        List<Service> activeServices = serviceManager.getServiceListByGroup(DUPLICATE_SERVICE_GROUP);
        for (Service s : activeServices) {
            try {
                List<String> storageRegionNodes = client.getChildren().forPath(ZkFileCoordinatorPaths.buildServiceSinkPath(s));
                if (storageRegionNodes.contains(storageRegionName)) {
                    result.add(s);
                }
            } catch (NoNodeException e) {
                LOG.info("no sink node for service[{}], region[{}] error", s.getServiceId(), storageRegionName);
            } catch (Exception e) {
                LOG.error("get sink for service[{}], region[{}] error", s.getServiceId(), storageRegionName, e);
            }
        }

        return result;
    }

    private long timeToLive(FileNode file) {
        return TimeUtils.nextTimeStamp(file.getCreateTime(), file.getTimeDurationMillis()) -
            TimeUtils.nextTimeStamp(System.currentTimeMillis(), file.getTimeDurationMillis());
    }

    private boolean handleFileNode(FileNode fileNode) {
        LOG.info("handling wild FileNode[{}]", JsonUtils.toJsonStringQuietly(fileNode));
        List<Service> serviceList = getServiceWithStorageRegionName(fileNode.getStorageName());
        Service target = serviceSelector.selectWith(fileNode, serviceList);
        if (target == null) {
            LOG.info("no service to accept filenode[{}], add it to wild list", fileNode.getName());
            return false;
        }

        LOG.info("transfer fileNode[{}] to service[{}]", fileNode.getName(), target.getServiceId());

        FileNode newFileNode = FileNode.newBuilder(fileNode)
                                       .setServiceId(target.getServiceId())
                                       .setServiceTime(target.getRegisterTime())
                                       .build();

        try {
            fileStorer.update(newFileNode);
        } catch (Exception e) {
            LOG.error("update file node[{}] info error", fileNode.getName(), e);
            return false;
        }

        try {
            // 在Sink中放入分配的文件名
            String path = client.create()
                                .forPath(ZkFileCoordinatorPaths.buildSinkFileNodePath(newFileNode),
                                         JsonUtils.toJsonBytes(newFileNode));
            LOG.info("filenode[{}] add to sink[{}]", newFileNode.getName(), path);

            return true;
        } catch (Exception e) {
            LOG.error("add filenode[{}] to sink error", newFileNode.getName(), e);
            try {
                fileStorer.update(fileNode);
            } catch (Exception e1) {
                LOG.error("roll back to original info of filenode[{}] error", fileNode.getName(), e1);
            }

            return false;
        }
    }

    private void dispatchWildFileNode() {
        LOG.info("wildlist[size:{}] will go new home !!", wildFileNodes.size());
        Iterator<FileNode> iter = wildFileNodes.iterator();
        while (iter.hasNext()) {
            if (handleFileNode(iter.next())) {
                iter.remove();
            }
        }
    }

    @Override
    public void serviceAdded(Service service) {
        LOG.info("Service added#######{}", service.getServiceId());
        serviceTimeTable.put(service.getServiceGroup(), service.getServiceId(), service.getRegisterTime());
        if (!childWatchers.containsKey(service.getServiceId())) {
            try {
                PathChildrenCache childWatcher =
                    new PathChildrenCache(client, ZkFileCoordinatorPaths.buildServiceSinkPath(service), false);
                childWatcher.getListenable().addListener(new PathChildrenCacheListener() {

                    @Override
                    public void childEvent(CuratorFramework client, PathChildrenCacheEvent event)
                        throws Exception {
                        LOG.info("happen event !!!");
                        executor.submit(new Runnable() {

                            @Override
                            public void run() {
                                dispatchWildFileNode();
                            }
                        });
                    }
                });

                childWatcher.start();
                childWatchers.put(service.getServiceId(), childWatcher);
            } catch (Exception e) {
                LOG.error("create path child cache error for {}", service, e);
            }
        }
    }

    @Override
    public void serviceRemoved(Service service) {
        LOG.info("Service removed#######{}", service.getServiceId());
        serviceTimeTable.remove(service.getServiceGroup(), service.getServiceId());
        PathChildrenCache childWatcher = childWatchers.get(service.getServiceId());
        CloseUtils.closeQuietly(childWatcher);

        //删除服务对应的文件槽
        try {
            client.delete()
                  .quietly()
                  .deletingChildrenIfNeeded()
                  .forPath(ZkFileCoordinatorPaths.buildServiceSinkPath(service));
        } catch (Exception e) {
            LOG.warn("Can not delete the sink of crushed service[{}]", service.getServiceId(), e);
        }

        //把崩溃的Service持有的文件节点放入列表
        executor.submit(new Runnable() {

            @Override
            public void run() {
                for (FileNode node : fileStorer.listFileNodes(new ServiceFileNodeFilter(service))) {
                    wildFileNodes.add(node);
                }
            }
        });
    }

    @Override
    public void timeExchanged(long startTime, Duration duration) {
        executor.submit(new Runnable() {

            @Override
            public void run() {
                LOG.debug("start to remove expired file node, time[{}]", startTime);
                Iterator<FileNode> iter = wildFileNodes.iterator();
                while (iter.hasNext()) {
                    FileNode file = iter.next();
                    if (timeToLive(file) >= 0) {
                        continue;
                    }

                    try {
                        LOG.info("close exipred file node[{}], create time[{}]", file.getName());
                        fileCloser.close(new FileObject(file), true);

                        iter.remove();
                    } catch (Exception e) {
                        LOG.error("close expired file node[{}] error.", file.getName());
                    }
                }
            }
        });
    }
}
