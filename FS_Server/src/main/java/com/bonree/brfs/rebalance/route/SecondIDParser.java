package com.bonree.brfs.rebalance.route;

import java.util.List;
import java.util.Map;

import com.bonree.brfs.common.rebalance.Constants;
import com.bonree.brfs.common.rebalance.route.NormalRoute;
import com.bonree.brfs.common.rebalance.route.VirtualRoute;
import com.bonree.brfs.common.utils.JsonUtils;
import com.bonree.brfs.common.utils.RebalanceUtils;
import com.bonree.brfs.common.zookeeper.curator.CuratorClient;

public class SecondIDParser {

    private int snID;
    private CuratorClient curatorClient;
    private String baseRoutesPath;
    private Map<String, NormalRoute> normalRouteDetail;

    private Map<String, VirtualRoute> virtualRouteDetail;

    public SecondIDParser(CuratorClient curatorClient, int snID, String baseRoutesPath) {
        this.snID = snID;
        this.curatorClient = curatorClient;
        this.baseRoutesPath = baseRoutesPath;
    }

    public void updateRoute() {
        // load virtual id
        String virtualPath = baseRoutesPath + Constants.SEPARATOR + Constants.VIRTUAL_ROUTE + Constants.SEPARATOR + snID;
        List<String> virtualNodes = curatorClient.getChildren(virtualPath);
        if (curatorClient.checkExists(virtualPath)) {
            if (virtualNodes != null && !virtualNodes.isEmpty()) {
                for (String virtualNode : virtualNodes) {
                    String dataPath = virtualPath + Constants.SEPARATOR + virtualNode;
                    byte[] data = curatorClient.getData(dataPath);
                    VirtualRoute virtual = JsonUtils.toObject(data, VirtualRoute.class);
                    virtualRouteDetail.put(virtual.getVirtualID(), virtual);
                }
            }
        }

        // load normal id
        String normalPath = baseRoutesPath + Constants.SEPARATOR + Constants.NORMAL_ROUTE + Constants.SEPARATOR + snID;
        if (curatorClient.checkExists(normalPath)) {
            List<String> normalNodes = curatorClient.getChildren(normalPath);
            if (normalNodes != null && !normalNodes.isEmpty()) {
                for (String normalNode : normalNodes) {
                    String dataPath = normalPath + Constants.SEPARATOR + normalNode;
                    byte[] data = curatorClient.getData(dataPath);
                    NormalRoute normal = JsonUtils.toObject(data, NormalRoute.class);
                    normalRouteDetail.put(normal.getSecondID(), normal);
                }
            }
        }
    }

    public String[] getAliveSecondID(String partfid) {
        String[] splitStr = partfid.split("_");
        String fileUUID = splitStr[0];
        
        String[] aliveArr = new String[splitStr.length - 1];

        for (int i = 1; i < splitStr.length; i++) { // 处理所有的副本
            String serverID = splitStr[i];
            if (serverID.charAt(0) == Constants.VIRTUAL_ID) {
                VirtualRoute virtualRoute = virtualRouteDetail.get(serverID);
                if (virtualRoute == null) { // 虚拟serverID还没有迁移哈，所以不变
                    aliveArr[i - 1] = serverID;
                } else {
                    serverID = virtualRoute.getNewSecondID(); // 找到迁移后的serverID
                    NormalRoute normalRoute = normalRouteDetail.get(serverID);// 查看该serverID是否迁移
                    List<String> newServerIDS = null;
                    while (normalRoute != null) { //只要发生了迁移，则继续计算迁移位置
                        newServerIDS = normalRoute.getNewSecondIDs();
//                        RebalanceUtils.newServerID(fileUUID, newServerIDS, fileServerIDs);
                    }
                }
            } else if (serverID.charAt(0) == Constants.MULTI_ID) {

            }
        }
        return null;
    }

}
