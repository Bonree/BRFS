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
import org.apache.curator.framework.recipes.cache.TreeCache;
import org.apache.curator.framework.recipes.cache.TreeCacheEvent;
import org.apache.curator.framework.recipes.cache.TreeCacheListener;
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
    private TreeCache treeCache;

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
        treeCache =
            TreeCache.newBuilder(this.client, zookeeperPaths.getBaseV2RoutePath())
                     .setCacheData(true)
                     .setExecutor(THREAD_FACTORY)
                     .build();
        treeCache.start();
        treeCache.getListenable().addListener(new RouteLister(zookeeperPaths.getBaseV2RoutePath()));
        LOG.info("route parser cache load ");
    }

    @LifecycleStop
    @Override
    public void stop() throws Exception {
        if (treeCache != null) {
            treeCache.close();
        }
        LOG.info("route parser cache stop ");
    }

    private class RouteLister implements TreeCacheListener {
        private String basePath;

        public RouteLister(String basePath) {
            this.basePath = basePath;
        }

        private BaseRoutePathInfo analysisPath(String basepath, String eventPath) {

            if (eventPath.equals(basepath)) {
                return new BaseRoutePathInfo(-1, false, "", RouteNodeType.ROOT);
            }
            String relativePath = eventPath.substring(basepath.length() + 1);
            String[] array = StringUtils.split(relativePath, Constants.SEPARATOR);
            String zkNode = "";
            int storageRegionId = -1;
            RouteNodeType type = RouteNodeType.INVALID;
            boolean virtualFlag = false;
            if (array == null) {
                return new BaseRoutePathInfo(storageRegionId, virtualFlag, zkNode, type);
            }
            if (array.length == 3) {
                storageRegionId = Integer.parseInt(array[1]);
                zkNode = array[2];
                if (Constants.VIRTUAL_ROUTE.equals(array[0])) {
                    type = RouteNodeType.VITRUAL_ROUTE;
                    virtualFlag = true;
                } else if (Constants.NORMAL_ROUTE.equals(array[0])) {
                    type = RouteNodeType.NORMAL_ROUTE;
                    virtualFlag = false;
                } else {
                    type = RouteNodeType.INVALID;
                }
            }
            if (array.length == 2) {
                storageRegionId = Integer.parseInt(array[1]);
                type = RouteNodeType.STORAGE_REGION;
            }
            if (array.length == 1) {
                if (Constants.VIRTUAL_ROUTE.equals(array[0])) {
                    type = RouteNodeType.VIRTUAL;
                    virtualFlag = true;
                } else if (Constants.NORMAL_ROUTE.equals(array[0])) {
                    type = RouteNodeType.NORMAL;
                    virtualFlag = false;
                } else {
                    type = RouteNodeType.INVALID;
                }
            }
            return new BaseRoutePathInfo(storageRegionId, virtualFlag, zkNode, type);
        }

        @Override
        public void childEvent(CuratorFramework client, TreeCacheEvent event) throws Exception {
            TreeCacheEvent.Type type = event.getType();
            switch (type) {
            case NODE_ADDED:
                ChildData childData = event.getData();
                String path = childData.getPath();
                if (childData == null) {
                    LOG.warn("path[{}] data is null", path);
                    return;
                }
                BaseRoutePathInfo baseRoutePathInfo = analysisPath(this.basePath, path);
                RouteNodeType nodeType = baseRoutePathInfo.getType();
                if (!RouteNodeType.NORMAL_ROUTE.equals(nodeType) && !RouteNodeType.VITRUAL_ROUTE.equals(nodeType)) {
                    LOG.warn("path is {},{}", path, nodeType);
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
                String typeName = null;
                if (baseRoutePathInfo.isVirtualFlag()) {
                    VirtualRoute route = SingleRouteFactory.createVirtualRoute(data);
                    routeParser.putVirtualRoute(route);
                    typeName = "virtual";

                } else {
                    NormalRouteInterface normal = SingleRouteFactory.createRoute(data);
                    routeParser.putNormalRoute(normal);
                    typeName = "normal";
                }
                LOG.info("load storageRegion [{}] type:[{}] routeId: [{}]", baseRoutePathInfo.getStorageRegionId(), typeName,
                         baseRoutePathInfo.getZkNodeName());
                break;
            default:
                LOG.info("event {}", type);
            }
        }
    }

    /**
     * 路由节点的类型
     */
    private enum RouteNodeType {
        ROOT,
        VIRTUAL,
        NORMAL,
        STORAGE_REGION,
        VITRUAL_ROUTE,
        NORMAL_ROUTE,
        INVALID
    }

    /**
     * 路由节点信息
     */
    private class BaseRoutePathInfo {
        private int storageRegionId;
        private boolean virtualFlag;
        private String zkNodeName;
        private RouteNodeType type;

        public BaseRoutePathInfo(int storageRegionId, boolean virtualFlag, String zkNodeName, RouteNodeType type) {
            this.storageRegionId = storageRegionId;
            this.virtualFlag = virtualFlag;
            this.zkNodeName = zkNodeName;
            this.type = type;
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

        public RouteNodeType getType() {
            return type;
        }
    }
}
