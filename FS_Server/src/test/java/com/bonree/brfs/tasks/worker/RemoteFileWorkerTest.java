package com.bonree.brfs.tasks.worker;

import com.bonree.brfs.common.ZookeeperPaths;
import com.bonree.brfs.common.service.Service;
import com.bonree.brfs.common.utils.BRFSPath;
import com.bonree.brfs.common.zookeeper.curator.cache.CuratorCacheFactory;
import com.bonree.brfs.duplication.storageregion.StorageRegion;
import com.bonree.brfs.duplication.storageregion.StorageRegionManager;
import com.bonree.brfs.duplication.storageregion.StorageRegionProperties;
import com.bonree.brfs.duplication.storageregion.StorageRegionStateListener;
import com.bonree.brfs.duplication.storageregion.impl.DefaultStorageRegionManager;
import com.bonree.brfs.identification.SecondMaintainerInterface;
import com.bonree.brfs.identification.VirtualServerID;
import com.bonree.brfs.identification.impl.SimpleSecondMaintainer;
import com.bonree.brfs.identification.impl.VirtualServerIDImpl;
import com.bonree.brfs.rebalance.route.RouteCache;
import com.bonree.brfs.rebalance.route.RouteLoader;
import com.bonree.brfs.rebalance.route.impl.RouteParserCache;
import com.bonree.brfs.rebalance.route.impl.SimpleRouteZKLoader;
import com.bonree.brfs.schedulers.task.manager.MetaTaskManagerInterface;
import com.bonree.brfs.schedulers.task.manager.impl.DefaultReleaseTask;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryNTimes;
import org.junit.Before;
import org.junit.Test;

public class RemoteFileWorkerTest {
    String zkAddress = "192.168.150.236:2181";
    String clusterName = "idea";
    CuratorFramework client = null;
    ZookeeperPaths zkPaths;
    MetaTaskManagerInterface release;
    StorageRegionManager manager;
    VirtualServerID virtualServerID = null;
    RouteParserCache routeCache;
    SecondMaintainerInterface secondMaintainerInterface;
    private Service local = null;
    private Service remote = null;

    @Before
    public void init() throws Exception {
        remote = new Service("11", "data_group", "192.168.150.237", 9901, 9991);
        remote.setExtraPort(9991);
        local = new Service("10", "data_group", "192.168.150.237", 9900, 9990);
        local.setExtraPort(9990);
        client = CuratorFrameworkFactory.newClient(zkAddress, new RetryNTimes(10, 1000));
        client.start();
        client.blockUntilConnected();
        zkPaths = ZookeeperPaths.getBasePath(clusterName, client);
        release = new DefaultReleaseTask(client, zkPaths);
        manager = new DefaultStorageRegionManager(client, zkPaths, null);
        manager.start();

        manager.addStorageRegionStateListener(new StorageRegionStateListener() {
            @Override
            public void storageRegionAdded(StorageRegion region) {

            }

            @Override
            public void storageRegionUpdated(StorageRegion region) {

            }

            @Override
            public void storageRegionRemoved(StorageRegion region) {

            }
        });
        CuratorCacheFactory.init(client);
        RouteLoader loader = new SimpleRouteZKLoader(client, zkPaths);
        routeCache = new RouteParserCache(loader, zkPaths, manager, client);
        routeCache.start();
        SimpleSecondMaintainer maintainerInterface = new SimpleSecondMaintainer(client, zkPaths, local);
        maintainerInterface.start();
        secondMaintainerInterface = maintainerInterface;
        virtualServerID = new VirtualServerIDImpl(client, zkPaths);
        Thread.sleep(10000);
    }

    @Test
    public void listFiles() throws Exception {
        StorageRegion region = new StorageRegion(
            "rebalance1", 0, 1591945256039L, true,
            2, "PT2H", 67108864L, "PT1M");
        RemoteFileWorker worker = new RemoteFileWorker(remote, secondMaintainerInterface, routeCache);
        Collection<BRFSPath> files = worker.listFiles(region);
        System.out.println(files);
        Map<BRFSPath, BRFSPath> map = new HashMap<>();
        files.forEach(
            file -> {
                map.put(file, file);
            }
        );
        worker.downloadFiles(map, "/data/br/tmp/data", 100);
    }
}
