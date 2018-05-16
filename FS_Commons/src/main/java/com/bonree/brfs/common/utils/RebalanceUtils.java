package com.bonree.brfs.common.utils;

import java.util.List;

public class RebalanceUtils {

    public static int hashFileName(String fileName, int size) {
        int nameSum = sumName(fileName);
        int matchSm = nameSum % size;
        return matchSm;
    }

    public static int sumName(String name) {
        int sum = 0;
        for (int i = 0; i < name.length(); i++) {
            sum = sum + name.charAt(i);
        }
        return sum;
    }

    public static boolean isAlive(String serverId, List<String> aliveServers) {
        if (aliveServers.contains(serverId)) {
            return true;
        } else {
            return false;
        }
    }

    /** 概述：判断是否需要恢复
     * @param serverIds
     * @param replicaPot
     * @return
     * @user <a href=mailto:weizheng@bonree.com>魏征</a>
     */
    public static boolean needRecover(List<String> serverIds, int replicaPot, List<String> aliveServers) {
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

    /** 概述：判断是否需要恢复
     * @param serverIds
     * @param replicaPot
     * @return
     * @user <a href=mailto:weizheng@bonree.com>魏征</a>
     */
    public static boolean needRecoverForAll(List<String> serverIds, int replicaPot, List<String> aliveServers) {
        boolean flag = false;
        for (int i = 1; i <= serverIds.size(); i++) {
            if (!aliveServers.contains(serverIds.get(i - 1))) {
                flag = true;
                break;
            }
        }
        return flag;
    }

}
