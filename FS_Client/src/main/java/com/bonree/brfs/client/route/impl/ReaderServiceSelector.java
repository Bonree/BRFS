package com.bonree.brfs.client.route.impl;

import java.util.List;
import java.util.Random;

import com.bonree.brfs.client.meta.ServiceMetaCache;
import com.bonree.brfs.client.route.RouteParser;
import com.bonree.brfs.client.route.ServiceMetaInfo;
import com.bonree.brfs.client.route.ServiceSelector_1;
import com.google.common.base.Preconditions;

public class ReaderServiceSelector implements ServiceSelector_1 {

    private final static String NAME_SEPARATOR = "_";

    @Override
    public ServiceMetaInfo selectService(ServiceMetaCache serviceCache,RouteParser routeParser,String partFid) {
        Preconditions.checkNotNull(partFid);
        List<String> aliveServices = serviceCache.listSecondID();
        ServiceMetaInfo service = null;
        String[] arrs = partFid.split(NAME_SEPARATOR);
        int paras = arrs.length;
        if (paras == 2) { // 一个副本
            System.out.println(arrs[1]);
            service = serviceCache.getFirstServerCache(arrs[1]);
        } else if (paras > 2) {// 多个副本时，选择一个副本
            int replicas = arrs.length - 1; // 除去UUID
            int random = new Random().nextInt(replicas) + 1;
            String selectSId = arrs[random];
            
            String aliveSecondID = routeParser.findServerID(selectSId, partFid, NAME_SEPARATOR, aliveServices);
            if (aliveSecondID != null) {
                service = serviceCache.getSecondServerCache(aliveSecondID,random);
            } else {
                for (int i = 0; i < replicas - 1; i++) {
                    random = ((random + 1) % replicas) + 1; // 尝试选择下一个service
                    selectSId = arrs[random];
                    aliveSecondID = routeParser.findServerID(selectSId, partFid, NAME_SEPARATOR, aliveServices);
                    if (aliveSecondID != null) {
                        service = serviceCache.getSecondServerCache(aliveSecondID,random);
                        break;
                    }
                }
            }
        }
        return service;
    }

}
