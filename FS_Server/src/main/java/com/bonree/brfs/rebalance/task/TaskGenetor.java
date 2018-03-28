package com.bonree.brfs.rebalance.task;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

import com.alibaba.fastjson.JSON;
import com.bonree.brfs.common.zookeeper.curator.CuratorClient;
import com.bonree.brfs.server.StorageName;

public class TaskGenetor {

    private String basePath = "/brfs/wz/rebalance/serverchange";

    private final static String SEPARATOR = "/";

    /** 概述：只需要添加每次变更的信息即可
     * @param zkUrl
     * @user <a href=mailto:weizheng@bonree.com>魏征</a>
     */
    public void addServers(String zkUrl) {
        genChangeSummary(zkUrl, ChangeType.ADD);
    }

    /** 概述：只需要添加每次变更的信息即可
     * @param zkUrl
     * @user <a href=mailto:weizheng@bonree.com>魏征</a>
     */
    public void removeServers(String zkUrl) {
        genChangeSummary(zkUrl, ChangeType.REMOVE);
    }

    private void genChangeSummary(String zkUrl, ChangeType type) {
        CuratorClient client = CuratorClient.getClientInstance(zkUrl);
        try {
            String changeServerId = "aaaaa";
            List<StorageName> snList = getStorageCache();
            for (StorageName snModel : snList) {
                if (snModel.getReplications() > 1 && snModel.isRecover()) {
                    List<String> currentServers = getCurrentServers();
                    ChangeSummary tsm = new ChangeSummary(snModel.getIndex(), Calendar.getInstance().getTimeInMillis() / 1000, type, changeServerId, currentServers);
                    String snPath = basePath + SEPARATOR + snModel.getIndex();
                    String jsonStr = JSON.toJSONString(tsm);
                    if (!client.checkExists(snPath)) {
                        client.createPersistent(snPath, false);
                    }
                    String snTaskNode = snPath + SEPARATOR + tsm.getCreateTime();
                    client.createPersistent(snTaskNode, false, jsonStr.getBytes());
                }

            }
        } finally {
            client.close();
        }
    }

    private List<StorageName> getStorageCache() {
        return new ArrayList<StorageName>();
    }

    private List<String> getCurrentServers() {
        return new ArrayList<String>();
    }
}
