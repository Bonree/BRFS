package com.bonree.brfs.client.route.impl;

import com.bonree.brfs.client.meta.impl.DiskServiceMetaCache;
import com.bonree.brfs.client.route.RouteParser;
import com.bonree.brfs.client.route.ServiceMetaInfo;
import com.bonree.brfs.common.service.Service;
import java.util.List;
import java.util.Random;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReaderServiceSelector {

    private static final Logger LOG = LoggerFactory.getLogger(ReaderServiceSelector.class);
    private DiskServiceMetaCache diskServiceMetaCache;
    private RouteParser routeParser;

    private Random rand = new Random();

    public ReaderServiceSelector(DiskServiceMetaCache diskServiceMetaCache, RouteParser routeParser) {
        this.diskServiceMetaCache = diskServiceMetaCache;
        this.routeParser = routeParser;
    }

    public ServiceMetaInfo selectService(String uuid, String[] serverIdList) {
        List<String> aliveServices = diskServiceMetaCache.listSecondID();
        LOG.debug("2260 get aliver second server!!!");

        if (serverIdList.length == 1) { // 一个副本
            LOG.debug("2260 this file has 1 replica,get the aliver server");
            return diskServiceMetaCache.getFirstServerCache(serverIdList[0]);
        }

        // 多个副本时，选择一个副本
        int index = rand.nextInt(serverIdList.length);
        for (int i = 0; i < serverIdList.length; i++) {
            if (serverIdList[index] != null) {
                break;
            }

            index = ++index % serverIdList.length;
        }

        final int pos = index + 1;
        String aliveSecondID = routeParser.findServerID(serverIdList[index], uuid, serverIdList, aliveServices);

        if (aliveSecondID != null) {
            return diskServiceMetaCache.getSecondServerCache(aliveSecondID, pos);
        }

        return new ServiceMetaInfo() {

            @Override
            public int getReplicatPot() {
                return pos;
            }

            @Override
            public Service getFirstServer() {
                return null;
            }
        };
    }

}
