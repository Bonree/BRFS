package com.bonree.brfs.client.route;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.curator.shaded.com.google.common.collect.Lists;

public class RouteRole {

    private int storageIndex;

    private Map<String, List<String>> routeDetail;

    private static Map<Integer, Map<String, List<String>>> RouteRoleCache = new ConcurrentHashMap<>();

    private RouteRole(int storageIndex) {
        this.storageIndex = storageIndex;
        routeDetail = new HashMap<>();
    }

    public void loadRouteRole() {
        routeDetail.put("1111", Lists.newArrayList("111"));
    }

    public static void loadRouteRole(int storageIndex) {
        RouteRole rr = new RouteRole(storageIndex);
        rr.loadRouteRole();
        RouteRoleCache.put(rr.getStorageIndex(), rr.getRouteDetail());
    }

    public int getStorageIndex() {
        return storageIndex;
    }

    public Map<String, List<String>> getRouteDetail() {
        return routeDetail;
    }

    public static Map<String, List<String>> getRouteRole(int storageIndex) {
        return RouteRoleCache.get(storageIndex);
    }

}
