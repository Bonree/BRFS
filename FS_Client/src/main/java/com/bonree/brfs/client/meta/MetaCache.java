package com.bonree.brfs.client.meta;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.bonree.brfs.client.netty.NettyServer;

public class MetaCache {

    public static Map<String, NettyServer> firstServerCache;

    public static Map<String, String> secondServerCache;

    public MetaCache() {
        firstServerCache = new ConcurrentHashMap<>();
        secondServerCache = new ConcurrentHashMap<>();
    }

    void loadCache() {

    }

    public NettyServer getFirstServerCache(String firstServerID) {
        return firstServerCache.get(firstServerID);
    }

    public NettyServer getSecondServerCache(String secondServerId) {
        String firstServerID = secondServerCache.get(secondServerId);
        if (firstServerID != null) {
            return firstServerCache.get(firstServerID);
        }
        return null;
    }

}
