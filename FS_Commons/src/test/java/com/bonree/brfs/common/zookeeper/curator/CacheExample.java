package com.bonree.brfs.common.zookeeper.curator;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.utils.ZKPaths;

import com.bonree.brfs.common.zookeeper.curator.CuratorClient;

public class CacheExample {

    private static final String PATH = "/brfs/wz/cache";

    public static void main(String[] args) throws Exception {
        CuratorClient client = CuratorClient.getClientInstance("192.168.101.86:2181");
        final Set<String> set = new HashSet<String>();
        PathChildrenCache cache = new PathChildrenCache(client.getInnerClient(), PATH, true);
        cache.start();

        PathChildrenCacheListener listener1 = new PathChildrenCacheListener() {

            @Override
            public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception {
                switch (event.getType()) {
                    case CHILD_ADDED: {
                        System.out.println("1Node added: " + ZKPaths.getNodeFromPath(event.getData().getPath()));
                        break;
                    }
                    case CHILD_UPDATED: {
                        System.out.println("1Node changed: " + ZKPaths.getNodeFromPath(event.getData().getPath()));
                        break;
                    }
                    case CHILD_REMOVED: {
                        System.out.println("1Node removed: " + ZKPaths.getNodeFromPath(event.getData().getPath()));
                        break;
                    }
                    default:
                        break;
                }
            }
        };
        
        
        PathChildrenCacheListener listener2 = new PathChildrenCacheListener() {

            @Override
            public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception {
                switch (event.getType()) {
                    case CHILD_ADDED: {
                        System.out.println("2Node added: " + ZKPaths.getNodeFromPath(event.getData().getPath()));
                        break;
                    }
                    case CHILD_UPDATED: {
                        System.out.println("2Node changed: " + ZKPaths.getNodeFromPath(event.getData().getPath()));
                        break;
                    }
                    case CHILD_REMOVED: {
                        System.out.println("2Node removed: " + ZKPaths.getNodeFromPath(event.getData().getPath()));
                        break;
                    }
                    default:
                        break;
                }
            }
        };
        ExecutorService services = Executors.newFixedThreadPool(5);
        cache.getListenable().addListener(listener1,services);
        cache.getListenable().addListener(listener2,services);
//        cache.getListenable().addListener(listener);
        Thread.sleep(Long.MAX_VALUE);
        
        cache.close();
        client.close();

    }

}
