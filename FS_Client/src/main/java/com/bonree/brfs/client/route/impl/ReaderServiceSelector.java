package com.bonree.brfs.client.route.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bonree.brfs.client.meta.impl.DiskServiceMetaCache;
import com.bonree.brfs.client.route.RouteParser;
import com.bonree.brfs.client.route.ServiceMetaInfo;
import com.bonree.brfs.client.route.ServiceSelector_1;
import com.google.common.base.Preconditions;

public class ReaderServiceSelector implements ServiceSelector_1 {

    private static final Logger LOG = LoggerFactory.getLogger(ReaderServiceSelector.class);
    private DiskServiceMetaCache diskServiceMetaCache;
    private RouteParser routeParser;

    private final static String NAME_SEPARATOR = "_";

    public ReaderServiceSelector(DiskServiceMetaCache diskServiceMetaCache, RouteParser routeParser) {
        this.diskServiceMetaCache = diskServiceMetaCache;
        this.routeParser = routeParser;
    }

    @Override
    public ServiceMetaInfo selectService(String partFid, List<Integer> excludePot) {
        Preconditions.checkNotNull(partFid);
        ServiceMetaInfo service = null;
        String aliveSecondID = null; // 迁移之后的serverID
        String selectSId = null; // 未迁移之前的serverID
        String[] arrs = partFid.split(NAME_SEPARATOR);
        if (excludePot == null) {
            excludePot = new ArrayList<Integer>(16);
        }

        if (excludePot.size() >= arrs.length - 1) {
            LOG.info("exclude all server!!!");
            return service;
        }
        List<String> aliveServices = diskServiceMetaCache.listSecondID();
        int paras = arrs.length;
        int pot = 1; // 默认位置
        if (paras == 2) { // 一个副本
            service = diskServiceMetaCache.getFirstServerCache(arrs[1]);
        } else if (paras > 2) {// 多个副本时，选择一个副本
            int replicas = arrs.length - 1; // 除去UUID
            int random = new Random().nextInt(replicas);
            pot = random + 1;
            if (!excludePot.contains(pot)) {
                selectSId = arrs[pot];
                LOG.info("select server id:" + selectSId);
                aliveSecondID = routeParser.findServerID(selectSId, partFid, NAME_SEPARATOR, aliveServices);
            }

            if (aliveSecondID != null) {
                service = diskServiceMetaCache.getSecondServerCache(aliveSecondID, pot);
            } else {
                for (int i = 0; i < replicas - 1; i++) {
                    random = ((random + 1) % replicas); // 尝试按顺序选择下一个service
                    pot = random + 1;
                    if (!excludePot.contains(pot)) {
                        selectSId = arrs[pot];
                        aliveSecondID = routeParser.findServerID(selectSId, partFid, NAME_SEPARATOR, aliveServices);
                        if (aliveSecondID != null) {
                            service = diskServiceMetaCache.getSecondServerCache(aliveSecondID, pot);
                            break;
                        }
                    }
                }
            }
        }
        return service;
    }

}
