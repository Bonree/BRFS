package com.bonree.brfs.duplication.filenode.duplicates;

import com.bonree.brfs.common.utils.JsonUtils;
import com.bonree.brfs.resourceschedule.model.ResourceModel;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;

/**
 * 获取系统
 */
public class ClusterResource implements Closeable{
    private static final Logger LOG = LoggerFactory.getLogger(ClusterResource.class);
    private Map<String, ResourceModel> clusterResources = new ConcurrentHashMap<>();
    private PathChildrenCache pathChildrenCache;
    private ExecutorService exector;
    private ClusterResource(PathChildrenCache pathChildrenCache, ExecutorService exector){
        this.pathChildrenCache = pathChildrenCache;
        this.exector = exector;
    }
    public ClusterResource start() throws Exception{
        this.pathChildrenCache.start();
        this.pathChildrenCache.getListenable().addListener(new ZkResource(),this.exector);
        LOG.info("daemon resource service work !!");
        return this;
    }

    /**
     * 获取资源信息
     * @return
     */
    public Map<String,ResourceModel> getClusterResourceMap(){
        return Collections.unmodifiableMap(clusterResources);
    }

    /**
     *
     * @return
     */
    public Collection<ResourceModel> getClusterResources(){
        return clusterResources.values();
    }

    @Override
    public void close() throws IOException{
        if(this.pathChildrenCache != null){
            this.pathChildrenCache.close();
        }
    }

    /**
     * 更新资源监听
     */
    private class ZkResource implements PathChildrenCacheListener{

        @Override
        public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception{
            PathChildrenCacheEvent.Type type = event.getType();
            ChildData data = event.getData();
            if(data == null) {
                LOG.warn("Event : {} ,ChildData is null", type);
                return;
            }
            byte[] content = data.getData();
            if(content == null || content.length == 0) {
                LOG.warn("Event : {} ,Byte data is null", type);
                return;
            }
            ResourceModel resource = JsonUtils.toObjectQuietly(content, ResourceModel.class);
            if(resource == null) {
                LOG.warn("Event : {} , Convert data is null", type);
                return;
            }
            String str = JsonUtils.toJsonString(resource);
            if(PathChildrenCacheEvent.Type.CHILD_ADDED.equals(type)
            || PathChildrenCacheEvent.Type.CHILD_UPDATED.equals(type)) {
                clusterResources.put(resource.getServerId(),resource);
            } else if(PathChildrenCacheEvent.Type.CHILD_REMOVED == type) {
                clusterResources.remove(resource.getServerId());
            } else {
                LOG.warn("event : {}, content:{}", type, str);
            }
        }
    }
    public static Builder newBuilder(){
      return new Builder();
    }

    /**
     * 创建集群监听进程
     */
    public static class Builder{
        /**
         * client客户端
         */
        private CuratorFramework client = null;
        /**
         * 监听路径
         */
        private String listenPath = null;
        /**
         * 是否在内存中缓存
         */
        private boolean cache =true;
        /**
         * 执行的线程池
         */
        private ExecutorService pool= null;
        public Builder(){

        }

        public Builder setClient(CuratorFramework client){
            this.client = client;
            return this;
        }

        public Builder setListenPath(String listenPath){
            this.listenPath = listenPath;
            return this;
        }

        public Builder setCache(boolean cache){
            this.cache = cache;
            return this;
        }

        public Builder setPool(ExecutorService pool){
            this.pool = pool;
            return this;
        }
        public ClusterResource build(){
            PathChildrenCache pathChildrenCache = new PathChildrenCache(client,listenPath,cache);
            return new ClusterResource(pathChildrenCache,pool);
        }
    }
}
