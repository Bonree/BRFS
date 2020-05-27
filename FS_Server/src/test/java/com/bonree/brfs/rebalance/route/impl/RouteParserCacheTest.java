package com.bonree.brfs.rebalance.route.impl;

import com.bonree.brfs.common.ZookeeperPaths;
import com.bonree.brfs.common.rebalance.Constants;
import com.bonree.brfs.common.rebalance.route.impl.v2.NormalRouteV2;
import com.bonree.brfs.common.utils.JsonUtils;
import com.bonree.brfs.duplication.storageregion.StorageRegion;
import com.bonree.brfs.duplication.storageregion.StorageRegionManager;
import com.bonree.brfs.duplication.storageregion.StorageRegionStateListener;
import com.bonree.brfs.duplication.storageregion.impl.DefaultStorageRegionManager;
import com.bonree.brfs.duplication.storageregion.impl.ZkStorageRegionIdBuilder;
import com.bonree.brfs.rebalance.route.RouteLoader;
import java.util.HashMap;
import java.util.Random;
import java.util.UUID;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryNTimes;
import org.apache.curator.utils.ZKPaths;
import org.apache.zookeeper.CreateMode;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RouteParserCacheTest {
    private static final Logger LOG = LoggerFactory.getLogger(RouteParserCacheTest.class);
    private CuratorFramework client = null;
    private String zkAddress = "192.168.150.236:2181";

    @Before
    public void init() throws Exception {
        client = CuratorFrameworkFactory.newClient(zkAddress, new RetryNTimes(10, 1000));
        client.start();
        client.blockUntilConnected();
    }

    @Test
    public void testLoad() throws Exception {
        ZookeeperPaths zookeeperPaths = ZookeeperPaths.create("idea", client);
        ZkStorageRegionIdBuilder builder = new ZkStorageRegionIdBuilder(client, zookeeperPaths);
        StorageRegionManager manager = new DefaultStorageRegionManager(client, zookeeperPaths, null);
        manager.start();

        StorageRegion region = new StorageRegion("abc", 0, System.currentTimeMillis(), true, 2, "PT2H", 3600, "PT2H");

        //manager.createStorageRegion(region.getName(),region.getProperties());
        RouteLoader loader = new SimpleRouteZKLoader(client, zookeeperPaths.getBaseV2RoutePath());
        RouteParserCache cache = new RouteParserCache(loader, zookeeperPaths, manager, client);
        cache.start();
        new Thread(
            new Runnable() {
                @Override
                public void run() {
                    for (int i = 0; i < 10; i++) {
                        try {
                            create(zookeeperPaths, 0);
                            Thread.sleep(100000);
                        } catch (Exception e) {
                            e.printStackTrace();
                        }

                    }
                }
            }
        ).start();
        Thread.sleep(Long.MAX_VALUE);
    }

    public void create(ZookeeperPaths zkPath, int storageIndex) throws Exception {
        String uuid = UUID.randomUUID().toString();
        String second = new Random(100).nextInt(100) + "";
        NormalRouteV2 normalRouteV2 = new NormalRouteV2(uuid, 0, second, new HashMap<>(), new HashMap<>());
        byte[] data = null;
        data = JsonUtils.toJsonBytes(normalRouteV2);
        String path = ZKPaths.makePath(zkPath.getBaseV2RoutePath(), Constants.NORMAL_ROUTE, storageIndex + "", uuid);
        String tmp = client.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).forPath(path, data);
        LOG.info("create {}", tmp);
    }
}
