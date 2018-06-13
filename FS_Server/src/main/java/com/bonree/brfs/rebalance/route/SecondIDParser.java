package com.bonree.brfs.rebalance.route;

import java.util.ArrayList;
import java.util.HashMap;
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
    private Map<String, NormalRoute> normalRouteDetail = new HashMap<>();
    private Map<String, VirtualRoute> virtualRouteDetail = new HashMap<>();

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

    public String[] getAliveSecondID(String partfid){
        try {
            String[] splitStr = partfid.split("_");
            String fileUUID = splitStr[0];
            List<String> fileServerIDs = new ArrayList<>(splitStr.length - 1);
            for (int i = 1; i < splitStr.length; i++) {
                fileServerIDs.add(splitStr[i]);
            }
            
            for (int i = 1; i < splitStr.length; i++) { // 处理所有的副本
                String serverID = splitStr[i];
                if (serverID.charAt(0) == Constants.VIRTUAL_ID) {
                    VirtualRoute virtualRoute = virtualRouteDetail.get(serverID);
                    if (virtualRoute != null) { // 副本发生了迁移
                        serverID = virtualRoute.getNewSecondID(); // 找到迁移后的serverID
                        fileServerIDs.set(i - 1, serverID);
                        NormalRoute normalRoute = normalRouteDetail.get(serverID);// 查看该serverID是否迁移
                        List<String> newServerIDS = null;
                        while (normalRoute != null) { // 只要发生了迁移，则继续计算迁移位置
                            newServerIDS = normalRoute.getNewSecondIDs();
                            serverID = RebalanceUtils.newServerID(fileUUID, newServerIDS, fileServerIDs);
                            fileServerIDs.set(i - 1, serverID);
                            normalRoute = normalRouteDetail.get(serverID);
                        }
                    }
                } else if (serverID.charAt(0) == Constants.MULTI_ID) {
                    NormalRoute normalRoute = normalRouteDetail.get(serverID);// 查看该serverID是否迁移
                    List<String> newServerIDS = null;
                    while (normalRoute != null) { // 只要发生了迁移，则继续计算迁移位置
                        newServerIDS = normalRoute.getNewSecondIDs();
                        serverID = RebalanceUtils.newServerID(fileUUID, newServerIDS, fileServerIDs);
                        fileServerIDs.set(i - 1, serverID);
                        normalRoute = normalRouteDetail.get(serverID);
                    }
                }
            }
            return fileServerIDs.toArray(new String[0]);
        }catch (Exception e) {
            throw new IllegalStateException("parse partFid error!!",e);
        }
    }
    
    public static void main(String[] args) {
        CuratorClient client =CuratorClient.getClientInstance("192.168.111.13:2181");
        SecondIDParser parser= new SecondIDParser(client, 0, "/brfs/bytest/routes");
        parser.updateRoute();
        parser.getAliveSecondID("f255abc796ea4da698b2e6b77f09c1cd_20_21_22");
    }

}
