package com.bonree.brfs.duplication.filenode.duplicates;

import com.bonree.brfs.common.utils.JsonUtils;
import com.bonree.brfs.resource.vo.ResourceModel;
import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 获取系统
 */
public class ClusterResource implements Closeable {
    private static final Logger LOG = LoggerFactory.getLogger(ClusterResource.class);
    private Map<String, ResourceModel> clusterResources = new ConcurrentHashMap<>();
    private PathChildrenCache pathChildrenCache;
    private ExecutorService exector;

    private ClusterResource(PathChildrenCache pathChildrenCache, ExecutorService exector) {
        this.pathChildrenCache = pathChildrenCache;
        this.exector = exector;
    }

    public void start() throws Exception {
        this.pathChildrenCache.start();
        this.pathChildrenCache.getListenable().addListener(new ResourceListener(), this.exector);
        LOG.info("resourceListener is listening resource changes from zk!!");
    }

    /**
     * 获取资源信息
     *
     * @return
     */
    public Map<String, ResourceModel> getClusterResourceMap() {
        return Collections.unmodifiableMap(clusterResources);
    }

    /**
     * @return
     */
    public Collection<ResourceModel> getClusterResources() {
        Collection<ResourceModel> result = new ArrayList<>();
        for (ResourceModel value : clusterResources.values()) {
            result.add(value);
        }
        return result;
    }

    @Override
    public void close() throws IOException {
        if (this.pathChildrenCache != null) {
            this.pathChildrenCache.close();
        }
    }

    /**
     * 集群资源监听器
     */
    private class ResourceListener implements PathChildrenCacheListener {

        @Override
        public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception {
            PathChildrenCacheEvent.Type type = event.getType();
            ChildData data = event.getData();
            if (data == null) {
                LOG.warn("Event : {} ,ChildData is null", type);
                return;
            }
            byte[] content = data.getData();
            if (content == null || content.length == 0) {
                LOG.warn("Event : {} ,Byte data is null", type);
                return;
            }
            ResourceModel resource = JsonUtils.toObjectQuietly(content, ResourceModel.class);
            if (resource == null) {
                LOG.warn("Event : {} , Convert data is null", type);
                return;
            }
            String str = JsonUtils.toJsonString(resource);
            if (PathChildrenCacheEvent.Type.CHILD_ADDED.equals(type)) {
                LOG.info("add a resource [{}] to cache", resource.getHost());
                clusterResources.put(resource.getServerId(), resource);
            } else if (PathChildrenCacheEvent.Type.CHILD_UPDATED.equals(type)) {
                clusterResources.put(resource.getServerId(), resource);
            } else if (PathChildrenCacheEvent.Type.CHILD_REMOVED == type) {
                LOG.info("remove a resource[{}] from cache", resource.getHost());
                clusterResources.remove(resource.getServerId());
            } else {
                LOG.warn("event : {}, content:{}", type, str);
            }
        }
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    /**
     * 创建集群监听进程
     */
    public static class Builder {
        /**
         * zk客户端
         */
        private CuratorFramework client = null;
        /**
         * 监听路径
         */
        private String listenPath = null;
        /**
         * 是否在内存中缓存
         */
        private boolean cache = true;
        /**
         * 执行的线程池
         */
        private ExecutorService pool = null;

        public Builder() {

        }

        public Builder setClient(CuratorFramework client) {
            this.client = client;
            return this;
        }

        public Builder setListenPath(String listenPath) {
            this.listenPath = listenPath;
            return this;
        }

        public Builder setCache(boolean cache) {
            this.cache = cache;
            return this;
        }

        public Builder setPool(ExecutorService pool) {
            this.pool = pool;
            return this;
        }

        public ClusterResource build() {
            PathChildrenCache pathChildrenCache = new PathChildrenCache(client, listenPath, cache);
            return new ClusterResource(pathChildrenCache, pool);
        }
    }
}
