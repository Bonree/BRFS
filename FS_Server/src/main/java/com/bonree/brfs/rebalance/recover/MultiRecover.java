package com.bonree.brfs.rebalance.recover;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSON;
import com.bonree.brfs.common.zookeeper.curator.CuratorClient;
import com.bonree.brfs.common.zookeeper.curator.cache.AbstractNodeCacheListener;
import com.bonree.brfs.common.zookeeper.curator.cache.CuratorCacheFactory;
import com.bonree.brfs.common.zookeeper.curator.cache.CuratorNodeCache;
import com.bonree.brfs.rebalance.Constants;
import com.bonree.brfs.rebalance.DataRecover;
import com.bonree.brfs.rebalance.record.BalanceRecord;
import com.bonree.brfs.rebalance.record.SimpleRecordWriter;
import com.bonree.brfs.rebalance.task.BalanceTaskSummary;
import com.bonree.brfs.rebalance.task.TaskDetail;
import com.bonree.brfs.rebalance.task.TaskStatus;
import com.bonree.brfs.server.StorageName;
import com.bonree.brfs.server.identification.ServerIDManager;

/*******************************************************************************
 * 版权信息：博睿宏远科技发展有限公司
 * Copyright: Copyright (c) 2007博睿宏远科技发展有限公司,Inc.All Rights Reserved.
 * 
 * @date 2018年4月9日 下午2:18:16
 * @Author: <a href=mailto:weizheng@bonree.com>魏征</a>
 * @Description: 副本恢复
 ******************************************************************************/
public class MultiRecover implements DataRecover {

    private Logger LOG = LoggerFactory.getLogger(MultiRecover.class);

    private BalanceTaskSummary balanceSummary;

    private ServerIDManager idManager;

    private final String taskNode;

    // 该SN历史迁移路由信息
    private Map<String, BalanceTaskSummary> snStorageSummary;

    private static final String NAME_SEPARATOR = "_";

    private CuratorNodeCache nodeCache;

    private final CuratorClient client;

    private boolean overFlag = false;

    private BlockingQueue<FileRecoverMeta> fileRecoverQueue = new ArrayBlockingQueue<>(2000);

    private AtomicReference<TaskStatus> status = new AtomicReference<TaskStatus>(TaskStatus.INIT);

    private class RecoverListener extends AbstractNodeCacheListener {

        public RecoverListener(String listenName) {
            super(listenName);
        }

        @Override
        public void nodeChanged() throws Exception {
            byte[] data = client.getData(taskNode);
            BalanceTaskSummary bts = JSON.parseObject(data, BalanceTaskSummary.class);
            TaskStatus stats = bts.getTaskStatus();
            // 更新缓存
            status.set(stats);
        }

    }

    public MultiRecover(BalanceTaskSummary summary, ServerIDManager idManager, String taskNode, CuratorClient client) {
        this.balanceSummary = summary;
        this.idManager = idManager;
        this.taskNode = taskNode;
        this.client = client;
        // 开启监控
        nodeCache = CuratorCacheFactory.getNodeCache();
        nodeCache.addListener(taskNode, new RecoverListener("recover"));
    }

    @Override
    public void recover() {
        LOG.info("begin recover");
        //启动消费队列
        new Thread(consumerQueue()).start();
        
        TaskDetail detail = new TaskDetail(idManager.getFirstServerID(), ExecutionStatus.INIT, 0, 0, 0);

        String selfNode = taskNode + Constants.SEPARATOR + idManager.getFirstServerID();

        detail.setStatus(ExecutionStatus.RECOVER);
        updateDetail(selfNode, detail);

        // 获取该sn的副本数
        int replicas = getStorageName(balanceSummary.getStorageIndex()).getReplications();

        LOG.info("deal the local server:" + idManager.getSecondServerID(balanceSummary.getStorageIndex()));

        // 以副本数来遍历
        for (int i = 1; i <= replicas; i++) {
            dealReplicas(i);
        }
        detail.setStatus(ExecutionStatus.FINISH);
        updateDetail(selfNode, detail);
        try {
            nodeCache.cancelListener(taskNode);
        } catch (IOException e) {
            LOG.error("cancel listener failed!!", e);
        }
        System.out.println("恢复完成");
        overFlag = true;
    }

    private void dealReplicas(int replica) {
        // 需要迁移的文件,按目录的时间从小到大处理
        List<String> repliFiles = getFiles();
        dealFiles(repliFiles, replica);
    }

    private void dealFiles(List<String> files, int replica) {
        SimpleRecordWriter simpleWriter = null;
        try {
            simpleWriter = new SimpleRecordWriter("");
            for (String perFile : files) {
                dealFile(perFile, replica, simpleWriter);
            }
        } catch (IOException e) {
            LOG.error("write balance record error!", e);
        } finally {
            if (simpleWriter != null) {
                try {
                    simpleWriter.close();
                } catch (IOException e) {
                    LOG.error("close simpleWriter error!", e);
                }
            }
        }

    }

    private void dealFile(String perFile, int replica, SimpleRecordWriter simpleWriter) throws IOException {
        // 对文件名进行分割处理
        String[] metaArr = perFile.split(NAME_SEPARATOR);
        // 提取出用于hash的部分
        String namePart = metaArr[0];

        // 提取出该文件所存储的服务
        List<String> fileServerIds = new ArrayList<>();
        for (int j = 1; j < metaArr.length; j++) {
            fileServerIds.add(metaArr[j]);
        }

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

        while (needRecover(fileServerIds, replica)) {
            for (String deadServer : fileServerIds) {
                if (!getAliveMultiIds().contains(deadServer)) {
                    int pot = fileServerIds.indexOf(deadServer);
                    if (!StringUtils.equals(deadServer, balanceSummary.getServerId())) {
                        recoverableServerList = getRecoverRoleList(deadServer);
                    } else {
                        recoverableServerList = balanceSummary.getInputServers();
                    }
                    exceptionServerIds = new ArrayList<>();
                    exceptionServerIds.addAll(fileServerIds);
                    exceptionServerIds.remove(deadServer);
                    selectableServerList = getSelectedList(recoverableServerList, exceptionServerIds);
                    int index = hashFileName(namePart, selectableServerList.size());
                    selectMultiId = selectableServerList.get(index);
                    fileServerIds.set(pot, selectMultiId);

                    // 判断选取的新节点是否存活
                    if (isAlive(selectMultiId)) {
                        // 判断选取的新节点是否为本节点
                        if (!idManager.getSecondServerID(balanceSummary.getStorageIndex()).equals(selectMultiId)) {
                            if (!isExistFile(selectMultiId, perFile)) {
                                remoteCopyFile(selectMultiId, perFile);
                                BalanceRecord record = new BalanceRecord(perFile, idManager.getSecondServerID(balanceSummary.getStorageIndex()), selectMultiId);
                                simpleWriter.writeRecord(record.toString());
                            }
                        }
                    }
                }
            }
        }
    }

    private Runnable consumerQueue() {
        return new Runnable() {

            @Override
            public void run() {
                try {
                    FileRecoverMeta fileRecover = null;

                    while (fileRecover != null || !overFlag) {
                        fileRecover = fileRecoverQueue.take();
                    }

                    System.out.println("transfer :" + fileRecover);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        };
    }

    private boolean isExistFile(String remoteServer, String fileName) {
        return true;
    }

    private void remoteCopyFile(String remoteServer, String fileName) {

    }

    private List<String> getFiles() {
        return new ArrayList<String>();
    }

    /** 概述：判断是否需要恢复
     * @param serverIds
     * @param replicaPot
     * @return
     * @user <a href=mailto:weizheng@bonree.com>魏征</a>
     */
    private boolean needRecover(List<String> serverIds, int replicaPot) {
        boolean flag = false;
        for (int i = 1; i <= serverIds.size(); i++) {
            if (i != replicaPot) {
                if (!getAliveMultiIds().contains(serverIds.get(i - 1))) {
                    flag = true;
                    break;
                }
            }
        }
        return flag;
    }

    private List<String> getRecoverRoleList(String serverModel) {
        return snStorageSummary.get(serverModel).getInputServers();
    }

    private List<String> getAliveMultiIds() {
        return balanceSummary.getAliveServer();
    }

    private List<String> getSelectedList(List<String> aliveServerList, List<String> excludeServers) {
        List<String> selectedList = new ArrayList<>();
        for (String tmp : aliveServerList) {
            if (!excludeServers.contains(tmp)) {
                selectedList.add(tmp);
            }
        }
        Collections.sort(selectedList, new CompareFromName());
        return selectedList;
    }

    private class CompareFromName implements Comparator<String> {
        @Override
        public int compare(String o1, String o2) {
            return o1.compareTo(o2);
        }
    }

    private int hashFileName(String fileName, int size) {
        int nameSum = sumName(fileName);
        int matchSm = nameSum % size;
        return matchSm;
    }

    private int sumName(String name) {
        int sum = 0;
        for (int i = 0; i < name.length(); i++) {
            sum = sum + name.charAt(i);
        }
        return sum;
    }

    private boolean isAlive(String serverId) {
        if (getAliveMultiIds().contains(serverId)) {
            return true;
        } else {
            return false;
        }
    }

    public StorageName getStorageName(int storageIndex) {
        return new StorageName();
    }

    /** 概述：更新任务信息
     * @param node
     * @param status
     * @user <a href=mailto:weizheng@bonree.com>魏征</a>
     */
    public void updateDetail(String node, TaskDetail detail) {
        if (client.checkExists(node)) {
            try {
                client.setData(node, JSON.toJSONString(detail).getBytes());
            } catch (Exception e) {
                LOG.error("change Task status error!", e);
            }
        }
    }

    /** 概述：注册节点
     * @param node
     * @user <a href=mailto:weizheng@bonree.com>魏征</a>
     */
    public void registerNode(String node, TaskDetail detail) {
        if (!client.checkExists(node)) {
            client.createPersistent(node, false, JSON.toJSONString(detail).getBytes());
        }
    }

}
