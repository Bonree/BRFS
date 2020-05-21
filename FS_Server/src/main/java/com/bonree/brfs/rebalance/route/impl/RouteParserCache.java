package com.bonree.brfs.rebalance.route.impl;

import com.bonree.brfs.common.ZookeeperPaths;
import com.bonree.brfs.common.lifecycle.LifecycleStart;
import com.bonree.brfs.common.lifecycle.LifecycleStop;
import com.bonree.brfs.common.lifecycle.ManageLifecycle;
import com.bonree.brfs.common.process.LifeCycle;
import com.bonree.brfs.common.rebalance.Constants;
import com.bonree.brfs.common.rebalance.route.NormalRouteInterface;
import com.bonree.brfs.common.rebalance.route.VirtualRoute;
import com.bonree.brfs.duplication.storageregion.StorageRegion;
import com.bonree.brfs.duplication.storageregion.StorageRegionManager;
import com.bonree.brfs.rebalance.route.BlockAnalyzer;
import com.bonree.brfs.rebalance.route.RouteCache;
import com.bonree.brfs.rebalance.route.RouteLoader;
import com.bonree.brfs.rebalance.route.factory.SingleRouteFactory;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.inject.Inject;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadFactory;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.codehaus.plexus.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ManageLifecycle
public class RouteParserCache implements RouteCache, LifeCycle {
    private static final Logger LOG = LoggerFactory.getLogger(RouteParserCache.class);
    private static final ThreadFactory THREAD_FACTORY = new ThreadFactoryBuilder().setNameFormat("RouteParserCache").build();
    private Map<Integer, RouteParser> analyzerMap = new ConcurrentHashMap<>();
    private RouteLoader loader;
    private ZookeeperPaths zookeeperPaths;
    private CuratorFramework client;
    private StorageRegionManager manager;
    private PathChildrenCache childrenCache;

    @Inject
    public RouteParserCache(RouteLoader loader, ZookeeperPaths zookeeperPaths, StorageRegionManager manager,
                            CuratorFramework client) {
        this.loader = loader;
        this.zookeeperPaths = zookeeperPaths;
        this.manager = manager;
        this.client = client;
    }

    @Override
    public BlockAnalyzer getBlockAnalyzer(int storageIndex) {
        if (analyzerMap.get(storageIndex) == null) {
            analyzerMap.put(storageIndex, new RouteParser(storageIndex, loader));
        }
        return analyzerMap.get(storageIndex);
    }

    @LifecycleStart
    @Override
    public void start() throws Exception {
        List<StorageRegion> regionList = manager.getStorageRegionList();
        if (regionList != null) {
            regionList.stream().forEach(region -> {
                RouteParser parser = new RouteParser(region.getId(), loader);
                LOG.info("load {} route", region.getName());
                analyzerMap.put(region.getId(), parser);
            });
        }
        childrenCache =
            new PathChildrenCache(this.client, zookeeperPaths.getBaseV2RoutePath(), true, THREAD_FACTORY);
        childrenCache.start();
        childrenCache.getListenable().addListener(new RouteLister(zookeeperPaths.getBaseV2RoutePath()));
        LOG.info("route parser cache load ");
    }

    @LifecycleStop
    @Override
    public void stop() throws Exception {
        if (childrenCache != null) {
            childrenCache.close();
        }
        LOG.info("route parser cache stop ");
    }

    private class RouteLister implements PathChildrenCacheListener {
        private String basePath;

        public RouteLister(String basePath) {
            this.basePath = basePath;
        }

        @Override
        public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception {
            PathChildrenCacheEvent.Type type = event.getType();
            ChildData childData = event.getData();
            if (childData == null) {
                return;
            }
            String path = childData.getPath();
            BaseRoutePathInfo baseRoutePathInfo = analysisPath(this.basePath, path);
            if (baseRoutePathInfo == null) {
                return;
            }
            byte[] data = childData.getData();
            if (data == null && data.length == 0) {
                return;
            }
            RouteParser routeParser = analyzerMap.get(baseRoutePathInfo.storageRegionId);
            if (routeParser == null) {
                routeParser = new RouteParser(baseRoutePathInfo.storageRegionId, loader, true);
                analyzerMap.put(baseRoutePathInfo.storageRegionId, routeParser);
            }
            switch (type) {
            case CHILD_ADDED:
                if (baseRoutePathInfo.isVirtualFlag()) {
                    VirtualRoute route = SingleRouteFactory.createVirtualRoute(data);
                    routeParser.putVirtualRoute(route);
                } else {
                    NormalRouteInterface normal = SingleRouteFactory.createRoute(data);
                    routeParser.putNormalRoute(normal);
                }
                LOG.info("load {} route ", baseRoutePathInfo.getStorageRegionId());
                break;
            default:
                LOG.info("event {}", type);
            }
        }

        private BaseRoutePathInfo analysisPath(String basepath, String eventPath) {
            String relativePath = eventPath.substring(basepath.length() + 1);
            String[] array = StringUtils.split(relativePath, Constants.SEPARATOR);
            if (array == null || array.length != 3) {
                return null;
            }
            String zkNode = array[2];
            int storageRegionId = Integer.parseInt(array[1]);
            boolean virtualFlag = Constants.VIRTUAL_ROUTE.equals(array[0]);
            return new BaseRoutePathInfo(storageRegionId, virtualFlag, zkNode);
        }
    }

    private class BaseRoutePathInfo {
        private int storageRegionId;
        private boolean virtualFlag;
        private String zkNodeName;

        public BaseRoutePathInfo(int storageRegionId, boolean virtualFlag, String zkNodeName) {
            this.storageRegionId = storageRegionId;
            this.virtualFlag = virtualFlag;
            this.zkNodeName = zkNodeName;
        }

        public int getStorageRegionId() {
            return storageRegionId;
        }

        public boolean isVirtualFlag() {
            return virtualFlag;
        }

        public String getZkNodeName() {
            return zkNodeName;
        }
    }
}
