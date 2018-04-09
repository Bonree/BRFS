package com.bonree.brfs.rebalance.recover;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bonree.brfs.rebalance.Constants;
import com.bonree.brfs.rebalance.DataRecover;
import com.bonree.brfs.rebalance.record.BalanceRecord;
import com.bonree.brfs.rebalance.record.SimpleRecordWriter;
import com.bonree.brfs.rebalance.task.BalanceTaskSummary;
import com.bonree.brfs.rebalance.task.TaskOperation;
import com.bonree.brfs.server.ServerInfo;
import com.bonree.brfs.server.StorageName;

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

    private StorageName storageName;

    private BalanceTaskSummary balanceSummary;

    private ServerInfo selfServerInfo;

    private TaskOperation taskOpt;

    private Map<String, BalanceTaskSummary> snStorageSummary;

    private static final String NAME_SEPARATOR = "_";

    public MultiRecover(BalanceTaskSummary summary, ServerInfo selfServerInfo, TaskOperation taskOpt) {
        this.balanceSummary = summary;
        this.selfServerInfo = selfServerInfo;
        this.taskOpt = taskOpt;
    }

    @Override
    public void recover() {
        LOG.info("begin recover");
        String node = Constants.PATH_TASKS + Constants.SEPARATOR + balanceSummary.getStorageIndex() + Constants.SEPARATOR + balanceSummary.getServerId() + Constants.SEPARATOR + selfServerInfo.getMultiIdentification();
        taskOpt.setTaskStatus(node, DataRecover.RUNNING_STAGE);
        int replicas = storageName.getReplications();

        LOG.info("deal the local server:" + selfServerInfo.getMultiIdentification());

        // 以副本数来遍历
        for (int i = 1; i <= replicas; i++) {
            dealReplicas(i);
        }
        taskOpt.setTaskStatus(node, DataRecover.FINISH_STAGE);
        System.out.println("恢复完成");
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
                        if (!selfServerInfo.getMultiIdentification().equals(selectMultiId)) {
                            if (!isExistFile(selectMultiId, perFile)) {
                                remoteCopyFile(selectMultiId, perFile);
                                BalanceRecord record = new BalanceRecord(perFile, selfServerInfo.getMultiIdentification(), selectMultiId);
                                simpleWriter.writeRecord(record.toString());
                            }
                        }
                    }
                }
            }
        }
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

}
