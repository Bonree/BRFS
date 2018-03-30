package com.bonree.brfs.rebalance.task;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

import com.alibaba.fastjson.JSON;
import com.bonree.brfs.common.zookeeper.curator.CuratorClient;
import com.bonree.brfs.rebalance.Constants;
import com.bonree.brfs.server.StorageName;

public class TaskGenetor {

    private String zkUrl;

    public TaskGenetor(String zkUrl) {
        this.zkUrl = zkUrl;
    }

    /** 概述：只需要添加每次变更的信息即可
     * @param zkUrl
     * @user <a href=mailto:weizheng@bonree.com>魏征</a>
     */
    public void addServers() {
        genChangeSummary(ChangeType.ADD);
    }

    /** 概述：只需要添加每次变更的信息即可
     * @param zkUrl
     * @user <a href=mailto:weizheng@bonree.com>魏征</a>
     */
    public void removeServers() {
        genChangeSummary(ChangeType.REMOVE);
    }

    private void genChangeSummary(ChangeType type) {
        CuratorClient client = CuratorClient.getClientInstance(zkUrl);
        try {
            String changeServerId = "server1";
            List<StorageName> snList = getStorageCache();
            for (StorageName snModel : snList) {
                if (snModel.getReplications() > 1 && snModel.isRecover()) {
                    List<String> currentServers = getCurrentServers();
                    ChangeSummary tsm = new ChangeSummary(snModel.getIndex(), Calendar.getInstance().getTimeInMillis() / 1000, type, changeServerId, currentServers);
                    String snPath = Constants.PATH_CHANGES + Constants.SEPARATOR + snModel.getIndex();
                    String jsonStr = JSON.toJSONString(tsm);
                    if (!client.checkExists(snPath)) {
                        client.createPersistent(snPath, true);
                    }
                    String snTaskNode = snPath + Constants.SEPARATOR + tsm.getCreateTime();
                    client.createPersistent(snTaskNode, false, jsonStr.getBytes());
                }

            }
        } finally {
            client.close();
        }
    }

    private List<StorageName> getStorageCache() {
        StorageName sn = new StorageName();
        sn.setIndex(1);
        sn.setStorageName("sdk");
        sn.setDescription("sdk");
        sn.setReplications(2);
        sn.setRecover(true);
        sn.setTtl(100000);
        sn.setTriggerRecoverTime(5454455544l);
        List<StorageName> tmp = new ArrayList<StorageName>();
        tmp.add(sn);
        return tmp;
    }

    private List<String> getCurrentServers() {
        List<String> servers = new ArrayList<String>();
        servers.add("server2");
        servers.add("server3");
        return servers;
    }

    public static void main(String[] args) {
        TaskGenetor tg = new TaskGenetor(Constants.zkUrl);
        tg.addServers();
    }
}
