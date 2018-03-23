package com.bonree.brfs.rebalance.task;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

import com.alibaba.fastjson.JSON;
import com.bonree.brfs.common.zookeeper.curator.CuratorClient;
import com.bonree.brfs.rebalance.task.model.TaskSummaryModel;
import com.bonree.brfs.server.model.StorageModel;

public class ServersMonitor {

    private String basePath = "/brfs/wz/rebalance/serverchange";

    private final static String SEPARATOR = "/";

    /** 概述：只需要添加每次变更的信息即可
     * @param zkUrl
     * @user <a href=mailto:weizheng@bonree.com>魏征</a>
     */
    public void addServers(String zkUrl) {
        CuratorClient client = CuratorClient.getClientInstance(zkUrl);
        try {
            ServerChangeType sct = ServerChangeType.ADD;
            String changeServerId = "aaaaa";

            List<StorageModel> snList = getStorageCache();
            for (StorageModel snModel : snList) {
                if (snModel.getReplications() > 1 && snModel.isRecover()) {
                    TaskSummaryModel tsm = new TaskSummaryModel();
                    tsm.setChangeServer(changeServerId);
                    tsm.setChangeType(sct);
                    List<String> currentServers = getCurrentServers();
                    tsm.setCurrentServers(currentServers);
                    tsm.setStorageIndex(snModel.getIndex());
                    tsm.setCreateTime(Calendar.getInstance().getTimeInMillis() / 1000);
                    String snPath = basePath + SEPARATOR + snModel.getIndex();
                    String jsonStr = JSON.toJSONString(tsm);
                    if (!client.checkExists(snPath)) {
                        client.createPersistent(snPath, false);
                    }
                    String snTaskNode = snPath + SEPARATOR + tsm.getCreateTime();
                    
                    client.createPersistent(snTaskNode, false,jsonStr.getBytes());
                }

            }
        } finally {
            client.close();
        }
    }
    
    /** 概述：只需要添加每次变更的信息即可
     * @param zkUrl
     * @user <a href=mailto:weizheng@bonree.com>魏征</a>
     */
    public void removeServers(String zkUrl) {
        CuratorClient client = CuratorClient.getClientInstance(zkUrl);
        try {
            ServerChangeType sct = ServerChangeType.REMOVE;
            String changeServerId = "aaaaa";

            List<StorageModel> snList = getStorageCache();
            for (StorageModel snModel : snList) {
                if (snModel.getReplications() > 1 && snModel.isRecover()) {
                    TaskSummaryModel tsm = new TaskSummaryModel();
                    tsm.setChangeServer(changeServerId);
                    tsm.setChangeType(sct);
                    List<String> currentServers = getCurrentServers();
                    tsm.setCurrentServers(currentServers);
                    tsm.setStorageIndex(snModel.getIndex());
                    tsm.setCreateTime(Calendar.getInstance().getTimeInMillis() / 1000);
                    String snPath = basePath + SEPARATOR + snModel.getIndex();
                    String jsonStr = JSON.toJSONString(tsm);
                    if (!client.checkExists(snPath)) {
                        client.createPersistent(snPath, false);
                    }
                    String snTaskNode = snPath + SEPARATOR + tsm.getCreateTime();
                    
                    client.createPersistent(snTaskNode, false,jsonStr.getBytes());
                }

            }
        } finally {
            client.close();
        }
    }

    private List<StorageModel> getStorageCache() {
        return new ArrayList<StorageModel>();
    }

    private List<String> getCurrentServers() {
        return new ArrayList<String>();
    }
}
