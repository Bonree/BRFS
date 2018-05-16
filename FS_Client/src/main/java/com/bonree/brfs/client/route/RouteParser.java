package com.bonree.brfs.client.route;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bonree.brfs.common.rebalance.Constants;
import com.bonree.brfs.common.rebalance.route.NormalRoute;
import com.bonree.brfs.common.rebalance.route.VirtualRoute;
import com.bonree.brfs.common.utils.RebalanceUtils;

public class RouteParser {

    private final static Logger LOG = LoggerFactory.getLogger(RouteParser.class);

    private RouteRoleCache routeCache;
    
    public RouteParser(RouteRoleCache routeCache) {
        this.routeCache = routeCache;
    }

    public String findServerID(String searchServerID, String fid, String separator, List<String> aliveServers) {

        // fid分为单副本serverID,多副本serverID,虚拟serverID。
        // 单副本不需要查找路由
        // 多副本需要查找路由，查找路由方式不同
        String secondID = null;
        if (Constants.VIRTUAL_ID == searchServerID.charAt(0)) {
            VirtualRoute virtualRoute = routeCache.getVirtualRoute(secondID);
            if (virtualRoute == null) {
                return secondID;
            }
            secondID = virtualRoute.getNewSecondID();
        }
        secondID = searchServerID;
        // 说明该secondID存活，不需要路由查找
        if (aliveServers.contains(secondID)) {
            return secondID;
        }

        // secondID不存活，需要寻找该secondID的存活ID
        NormalRoute routeRole = routeCache.getRouteRole(secondID);
        if (routeRole == null) { // 若没有迁移记录，可能没有迁移完成
            return null;
        }

        // 对文件名进行分割处理
        String[] metaArr = fid.split(separator);
        // 提取出用于hash的部分
        String namePart = metaArr[0];
        // 提取副本数
        int replicas = metaArr.length - 1;

        // 提取出该文件所存储的服务
        List<String> fileServerIds = new ArrayList<>();
        for (int j = 1; j < metaArr.length; j++) {
            // virtual server ID
            if (Constants.VIRTUAL_ID == metaArr[j].charAt(0)) {
                if (metaArr[j].equals(searchServerID)) { // 前面解析过
                    fileServerIds.add(secondID);
                } else { // 需要解析
                    VirtualRoute virtualRoute = routeCache.getVirtualRoute(secondID);
                    if (virtualRoute == null) {
                        LOG.error("gain serverid error!something impossible!!!");
                        return null;
                    }
                    fileServerIds.add(virtualRoute.getNewSecondID());
                }
            }
        }
        // 提取需要查询的serverID的位置
        int serverIDPot = fileServerIds.indexOf(secondID);

        // 这里要判断一个副本是否需要进行迁移
        // 挑选出的可迁移的servers
        String selectMultiId = null;
        // 可获取的server，可能包括自身
        List<String> recoverableServerList = null;
        // 排除掉自身或已有的servers
        List<String> exceptionServerIds = null;
        // 真正可选择的servers
        List<String> selectableServerList = null;

        while (RebalanceUtils.needRecover(fileServerIds, replicas, aliveServers)) {
            for (String deadServer : fileServerIds) {
                if (!aliveServers.contains(deadServer)) {
                    int pot = fileServerIds.indexOf(deadServer);
                    NormalRoute newRoute = routeCache.getRouteRole(deadServer);
                    if (newRoute == null) {
                        return null;
                    }
                    recoverableServerList = newRoute.getNewSecondIDs();
                    exceptionServerIds = new ArrayList<>();
                    exceptionServerIds.addAll(fileServerIds);
                    exceptionServerIds.remove(deadServer);
                    selectableServerList = RebalanceUtils.getSelectedList(recoverableServerList, exceptionServerIds);
                    int index = RebalanceUtils.hashFileName(namePart, selectableServerList.size());
                    selectMultiId = selectableServerList.get(index);
                    fileServerIds.set(pot, selectMultiId);

                    // 判断选取的新节点是否存活
                    if (RebalanceUtils.isAlive(selectMultiId, aliveServers)) {
                        // 判断选取的新节点是否为本节点，该serverID是否在相应的位置
                        if (pot == serverIDPot) {
                            break;
                        }
                    }
                }
            }
        }
        return selectMultiId;
    }

}
