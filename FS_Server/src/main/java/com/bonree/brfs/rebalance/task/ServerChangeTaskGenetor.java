package com.bonree.brfs.rebalance.task;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSON;
import com.bonree.brfs.common.zookeeper.curator.CuratorClient;
import com.bonree.brfs.rebalance.Constants;
import com.bonree.brfs.server.ServerInfo;
import com.bonree.brfs.server.StorageName;

public class ServerChangeTaskGenetor {

    private static final Logger LOG = LoggerFactory.getLogger(ServerChangeTaskGenetor.class);

    private LeaderLatch leaderLath;

    private String leaderPath;

    private String changesPath;

    private CuratorClient client; // TODO 可以替换成zkURL，每次变更时，在生成新的client。

    public ServerChangeTaskGenetor(final CuratorClient client, final String leaderPath, final String changesNode) throws Exception {
        this.leaderPath = leaderPath;
        this.changesPath = changesNode;
        this.client = client;
        this.leaderLath = new LeaderLatch(client.getInnerClient(), this.leaderPath);
        leaderLath.start();
        LOG.info("ServerChangeTaskGenetor launch successful!!");
    }

    /** 概述：只需要添加每次变更的信息即可
     * @param zkUrl
     * @user <a href=mailto:weizheng@bonree.com>魏征</a>
     */
    public void addServers(ServerInfo changeServer) {
        genChangeSummary(changeServer, ChangeType.ADD);
    }

    /** 概述：只需要添加每次变更的信息即可
     * @param zkUrl
     * @user <a href=mailto:weizheng@bonree.com>魏征</a>
     */
    public void removeServers(ServerInfo changeServer) {
        genChangeSummary(changeServer, ChangeType.REMOVE);
    }

    private void genChangeSummary(ServerInfo changeServer, ChangeType type) {
        String changeServerId = changeServer.getMultiIdentification();
        List<StorageName> snList = getStorageCache();
        for (StorageName snModel : snList) {
            if (snModel.getReplications() > 1 && snModel.isRecover()) {
                List<String> currentServers = getCurrentServers();
                ChangeSummary tsm = new ChangeSummary(snModel.getIndex(), Calendar.getInstance().getTimeInMillis() / 1000, type, changeServerId, currentServers);
                String snPath = changesPath + Constants.SEPARATOR + snModel.getIndex();
                String jsonStr = JSON.toJSONString(tsm);
                if (!client.checkExists(snPath)) {
                    client.createPersistent(snPath, true);
                }
                String snTaskNode = snPath + Constants.SEPARATOR + tsm.getCreateTime();
                client.createPersistent(snTaskNode, false, jsonStr.getBytes());
            }
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

        // TaskGenetor tg = new TaskGenetor(Constants.zkUrl);
        // tg.addServers();
    }
}
