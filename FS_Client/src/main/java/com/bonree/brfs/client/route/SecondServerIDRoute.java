package com.bonree.brfs.client.route;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

public class SecondServerIDRoute {

    private final static String NAME_SEPARATOR = "_";

    private SecondServerIDRoute() {
    }

    public static String findServerID(int storageIndex, String secondServerID, String fid, List<String> aliveServers) {

        Map<String, List<String>> routeRole = RouteRole.getRouteRole(storageIndex);
        // 无迁移记录，说明该serverID没有进行过迁移
        if (routeRole == null || routeRole.isEmpty()) {
            return secondServerID;
        }
        int replica = 2; // TODO 获取副本数
        int serverIDPot = 0;
        // 对文件名进行分割处理
        String[] metaArr = fid.split(NAME_SEPARATOR);
        // 提取出用于hash的部分
        String namePart = metaArr[0];

        // 提取出该文件所存储的服务
        List<String> fileServerIds = new ArrayList<>();
        for (int j = 1; j < metaArr.length; j++) {
            fileServerIds.add(metaArr[j]);
        }
        serverIDPot = fileServerIds.indexOf(secondServerID);
        // 此处需要将有virtual Serverid的文件进行转换

        // 这里要判断一个副本是否需要进行迁移
        // 挑选出的可迁移的servers
        String selectMultiId = null;
        // 可获取的server，可能包括自身
        List<String> recoverableServerList = null;
        // 排除掉自身或已有的servers
        List<String> exceptionServerIds = null;
        // 真正可选择的servers
        List<String> selectableServerList = null;

        while (needRecover(fileServerIds, replica, aliveServers)) {
            for (String deadServer : fileServerIds) {
                if (!aliveServers.contains(deadServer)) {
                    int pot = fileServerIds.indexOf(deadServer);
                    recoverableServerList = routeRole.get(deadServer);
                    exceptionServerIds = new ArrayList<>();
                    exceptionServerIds.addAll(fileServerIds);
                    exceptionServerIds.remove(deadServer);
                    selectableServerList = getSelectedList(recoverableServerList, exceptionServerIds);
                    int index = hashFileName(namePart, selectableServerList.size());
                    selectMultiId = selectableServerList.get(index);
                    fileServerIds.set(pot, selectMultiId);

                    // 判断选取的新节点是否存活
                    if (isAlive(selectMultiId, aliveServers)) {
                        // 判断选取的新节点是否为本节点，该serverID是否在相应的位置
                        if (pot == serverIDPot) {
                            secondServerID = selectMultiId;
                        }
                    }
                }
            }
        }
        return secondServerID;
    }

    /** 概述：判断是否需要恢复
     * @param serverIds
     * @param replicaPot
     * @return
     * @user <a href=mailto:weizheng@bonree.com>魏征</a>
     */
    private static boolean needRecover(List<String> serverIds, int replicaPot, List<String> aliveServers) {
        boolean flag = false;
        for (int i = 1; i <= serverIds.size(); i++) {
            if (i != replicaPot) {
                if (!aliveServers.contains(serverIds.get(i - 1))) {
                    flag = true;
                    break;
                }
            }
        }
        return flag;
    }

    private static List<String> getSelectedList(List<String> aliveServerList, List<String> excludeServers) {
        List<String> selectedList = new ArrayList<>();
        for (String tmp : aliveServerList) {
            if (!excludeServers.contains(tmp)) {
                selectedList.add(tmp);
            }
        }
        Collections.sort(selectedList, new CompareFromName());
        return selectedList;
    }

    static private class CompareFromName implements Comparator<String> {
        @Override
        public int compare(String o1, String o2) {
            return o1.compareTo(o2);
        }
    }

    private static int hashFileName(String fileName, int size) {
        int nameSum = sumName(fileName);
        int matchSm = nameSum % size;
        return matchSm;
    }

    private static int sumName(String name) {
        int sum = 0;
        for (int i = 0; i < name.length(); i++) {
            sum = sum + name.charAt(i);
        }
        return sum;
    }

    private static boolean isAlive(String serverId, List<String> aliveServers) {
        if (aliveServers.contains(serverId)) {
            return true;
        } else {
            return false;
        }
    }

}
