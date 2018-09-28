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

    public String findServerID(String searchServerID, String namePart, List<String> serverIds, List<String> aliveServers) {

        // fid分为单副本serverID,多副本serverID,虚拟serverID。
        // 单副本不需要查找路由
        // 多副本需要查找路由，查找路由方式不同
        String secondID = searchServerID;
        if (Constants.VIRTUAL_ID == secondID.charAt(0)) {
            LOG.debug("2260 a virtual serverid :" + secondID);
            VirtualRoute virtualRoute = routeCache.getVirtualRoute(secondID);
            if (virtualRoute == null) {
                LOG.debug("2260 no virtualRoute:" + virtualRoute);
                return null; // 虚拟serverID没有进行迁移，所以返回null，标识找不到可用的server
            }
            secondID = virtualRoute.getNewSecondID();
            LOG.debug("2260 newsecondid:" + secondID);
        }

        // 说明该secondID存活，不需要路由查找
        LOG.info("2260 aliveServers : " + aliveServers);
        if (aliveServers.contains(secondID)) {
            LOG.debug("2260 aliver secondid:" + secondID);
            return secondID;
        }
        LOG.debug("2260 dead secondID:" + secondID);
        // secondID不存活，需要寻找该secondID的存活ID
        NormalRoute routeRole = routeCache.getRouteRole(secondID);
        if (routeRole == null) { // 若没有迁移记录，可能没有迁移完成
            LOG.debug("2260 no normalRoute:" + routeRole);
            return null; // 不是存活的secondid，并没有发生迁移，返回null，标识不可用
        }

        // 提取副本数
        int replicas = serverIds.size();
        // 提取出该文件所存储的服务
        List<String> fileServerIds = new ArrayList<>();

        for (String serverId : serverIds) {
        	if(serverId == null) {
        		continue;
        	}
        	
            // virtual server ID 分为已经解析或者还未解析的
            if (Constants.VIRTUAL_ID == serverId.charAt(0)) {
                if (serverId.equals(searchServerID)) { // 前面解析过
                    fileServerIds.add(secondID);
                } else { // 需要解析
                    VirtualRoute virtualRoute = routeCache.getVirtualRoute(secondID);
                    if (virtualRoute == null) { // 如果有普通迁移，那么肯定无虚拟迁移，或者虚拟迁移都已经进行完成
                        LOG.error("2260 gain serverid error!something impossible!!!");
                        return null;
                    }
                    fileServerIds.add(virtualRoute.getNewSecondID());
                }
            }
            LOG.debug("2260 construct a source second server id list :" + fileServerIds);
        }
        // 提取需要查询的serverID的位置
        final int serverIDPot = fileServerIds.indexOf(secondID);
        LOG.debug("2260 this server id's pot is :" + serverIDPot);

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
            LOG.info("2260 check need to route,fileServerIDs:" + fileServerIds + "----->" + "aliverServers:" + aliveServers);
            for (String deadServer : fileServerIds) { // 将所有挂掉的服务尝试恢复一次
                if (!aliveServers.contains(deadServer)) { // 尝试去路由该挂掉的服务的新服务中
                    int pot = fileServerIds.indexOf(deadServer);
                    NormalRoute newRoute = routeCache.getRouteRole(deadServer);
                    if (newRoute == null) {
                        LOG.debug("2260 deadServer:" + deadServer + ",is no finished the recover!!!");
                        int deadIndex = fileServerIds.indexOf(deadServer);
                        if (deadIndex == serverIDPot) {
                            LOG.debug("2260 a deadServer no finished the recover,but try to read data from the second's server:" + deadServer);
                            return null;
                        } else {
                            continue;
                        }
                    }
                    LOG.debug("2260 normal route:" + newRoute);
                    recoverableServerList = newRoute.getNewSecondIDs();
                    exceptionServerIds = new ArrayList<>();
                    exceptionServerIds.addAll(fileServerIds);
                    exceptionServerIds.remove(deadServer);
                    selectableServerList = RebalanceUtils.getSelectedList(recoverableServerList, exceptionServerIds);
                    int index = RebalanceUtils.hashFileName(namePart, selectableServerList.size());
                    selectMultiId = selectableServerList.get(index);
                    LOG.debug("2260 partFile:" + namePart + "," + "deadserver:" + deadServer + "to " + "newServer:" + selectMultiId);
                    fileServerIds.set(pot, selectMultiId);

                    // 判断选取的新节点是否存活
                    if (RebalanceUtils.isAlive(selectMultiId, aliveServers)) {
                        // 判断选取的新节点是否为本节点，该serverID是否在相应的位置
                        if (pot == serverIDPot) {
                            LOG.debug("2260 select a right server id :" + selectMultiId + ", for indexPot:" + serverIDPot);
                            break;
                        }
                    }
                }
            }
        }
        return selectMultiId;
    }

}
