package com.bonree.brfs.client.route.impl;

import java.util.List;
import java.util.Random;

import com.bonree.brfs.client.meta.impl.DiskServiceMetaCache;
import com.bonree.brfs.client.route.RouteParser;
import com.bonree.brfs.client.route.ServiceMetaInfo;
import com.bonree.brfs.client.route.ServiceSelector_1;
import com.google.common.base.Preconditions;

public class ReaderServiceSelector implements ServiceSelector_1 {
    private DiskServiceMetaCache diskServiceMetaCache;
    private RouteParser routeParser;

    private final static String NAME_SEPARATOR = "_";

    public ReaderServiceSelector(DiskServiceMetaCache diskServiceMetaCache, RouteParser routeParser) {
        this.diskServiceMetaCache = diskServiceMetaCache;
        this.routeParser = routeParser;
    }

    @Override
    public ServiceMetaInfo selectService(String partFid) {
        Preconditions.checkNotNull(partFid);
        List<String> aliveServices = diskServiceMetaCache.listSecondID();
        ServiceMetaInfo service = null;
        String[] arrs = partFid.split(NAME_SEPARATOR);
        int paras = arrs.length;
        if (paras == 2) { // 一个副本
            service = diskServiceMetaCache.getFirstServerCache(arrs[1]);
        } else if (paras > 2) {// 多个副本时，选择一个副本
            int replicas = arrs.length - 1; // 除去UUID
            int random = new Random().nextInt(replicas) + 1;
            String selectSId = arrs[random];

            String aliveSecondID = routeParser.findServerID(selectSId, partFid, NAME_SEPARATOR, aliveServices);
            if (aliveSecondID != null) {
                service = diskServiceMetaCache.getSecondServerCache(aliveSecondID, random);
            } else {
                for (int i = 0; i < replicas - 1; i++) {
                    random = ((random + 1) % replicas) + 1; // 尝试选择下一个service
                    selectSId = arrs[random];
                    aliveSecondID = routeParser.findServerID(selectSId, partFid, NAME_SEPARATOR, aliveServices);
                    if (aliveSecondID != null) {
                        service = diskServiceMetaCache.getSecondServerCache(aliveSecondID, random);
                        break;
                    }
                }
            }
        }
        return service;
    }

}
