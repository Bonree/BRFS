package com.bonree.brfs.client.route.impl;

import java.util.List;
import java.util.Random;

import com.bonree.brfs.client.meta.ServiceMetaCache;
import com.bonree.brfs.client.route.RouteParser;
import com.bonree.brfs.client.route.ServiceSelector;
import com.bonree.brfs.common.service.Service;
import com.google.common.base.Preconditions;

public class SimpleServiceSelector implements ServiceSelector {

    private final static String NAME_SEPARATOR = "_";

    private ServiceMetaCache serviceCache;

    private RouteParser routeParser;

    public enum SelectorType {
        CREATE_SN, UPDATE_SN, DELETE_SN, DELETE_DATA, WRITE_DATA, READ_DATA
    }

    private SimpleServiceSelector(final String zkHosts, final String zkServerIDPath, final int snIndex, final String baseRoutePath) {
        serviceCache = new ServiceMetaCache(zkHosts, zkServerIDPath, snIndex);
        routeParser = new RouteParser(zkHosts, snIndex, baseRoutePath);
    }

    /** 概述：writer数据时，和service的硬盘有关联
     * @return
     * @user <a href=mailto:weizheng@bonree.com>魏征</a>
     */
    @Override
    public Service selectWriteService() {
        String perfectFirstID = null;
        return serviceCache.getFirstServerCache(perfectFirstID);
    }

    /** 概述：根据部分fid，来获取
     * @param partFid
     * @return
     * @user <a href=mailto:weizheng@bonree.com>魏征</a>
     */
    @Override
    public Service selectReadService(String partFid) {
        Preconditions.checkNotNull(partFid);
        List<String> aliveServices = serviceCache.listSecondID();
        Service service = null;
        String[] arrs = partFid.split(NAME_SEPARATOR);
        int paras = arrs.length;
        if (paras == 2) { // 一个副本
            service = serviceCache.getFirstServerCache(arrs[1]);
        } else if (paras > 2) {// 多个副本时，选择一个副本
            int replicas = arrs.length - 1; // 除去UUID
            int random = new Random().nextInt(replicas);
            String selectSId = arrs[random + 1];
            String aliveSecondID = routeParser.findServerID(selectSId, partFid, NAME_SEPARATOR, aliveServices);
            if (aliveSecondID != null) {
                service = serviceCache.getSecondServerCache(aliveSecondID);
            } else {
                for (int i = 0; i < replicas - 1; i++) {
                    random = (random + 1) % replicas; // 尝试选择下一个service
                    selectSId = arrs[random + 1];
                    aliveSecondID = routeParser.findServerID(selectSId, partFid, NAME_SEPARATOR, aliveServices);
                    if (aliveSecondID != null) {
                        service = serviceCache.getSecondServerCache(aliveSecondID);
                        break;
                    }
                }
            }
        }
        return service;
    }

    /** 概述：采用随机的方式
     * @return
     * @user <a href=mailto:weizheng@bonree.com>魏征</a>
     */
    @Override
    public Service selectRandomService() {
        return serviceCache.getRandomService();
    }
}
