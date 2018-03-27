package com.bonree.brfs.rebalance.recover;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bonree.brfs.rebalance.DataRecover;
import com.bonree.brfs.server.model.ServerInfoModel;
import com.bonree.brfs.server.model.StorageName;

public class MultiRecover implements DataRecover {

    private Logger LOG = LoggerFactory.getLogger(MultiRecover.class);

    private StorageName storageName;

    private static final String NAME_SEPARATOR = "_";

    public MultiRecover(StorageName storageName) {
        this.storageName = storageName;
    }

    @Override
    public void recover() {
        LOG.info("begin recover");
        int replicas = storageName.getReplications();

        // 获取本地ServerInfo信息
        ServerInfoModel localServer = new ServerInfoModel();
        String localMultiId = localServer.getMultiIdentification();

        System.out.println("处理" + localMultiId);
        LOG.info("deal the local server:" + localMultiId);
        //以副本数来遍历
        for (int i = 1; i <= replicas; i++) {
            
            List<String> repliFiles = getFiles();
            for (String perFile : repliFiles) {
                String[] metaArr = perFile.split(NAME_SEPARATOR);
                String namePart = metaArr[0];
                List<String> aliveServerIdList = new ArrayList<>();
                for (int j = 1; j < metaArr.length; j++) {
                    aliveServerIdList.add(metaArr[j]);
                }
                // 这里要判断一个副本是否需要进行迁移
                ServerInfoModel selectServerModel = null;
                List<ServerInfoModel> recoverableServerList = null;
                List<String> exceptionServerIds = null;
                List<ServerInfoModel> selectableServerList = null;
                while (needRecover(aliveServerIdList, i)) {
                    for (String deadServer : getDeadMultiIds()) {
                        if (aliveServerIdList.contains(deadServer)) {
                            int pot = aliveServerIdList.indexOf(deadServer);
                            recoverableServerList = getRecoverRoleList(deadServer);
                            exceptionServerIds = new ArrayList<>();
                            exceptionServerIds.addAll(aliveServerIdList);
                            exceptionServerIds.remove(deadServer);
                            selectableServerList = getSelectedList(recoverableServerList, exceptionServerIds);
                            int index = hashFileName(namePart, selectableServerList.size());
                            selectServerModel = selectableServerList.get(index);
                            aliveServerIdList.set(pot, selectServerModel.getMultiIdentification());

                            // 判断选取的新节点是否存活
                            if (isAlive(selectServerModel.getMultiIdentification())) {
                                // 判断选取的新节点是否为本节点
                                if (!localServer.getMultiIdentification().equals(selectServerModel.getMultiIdentification())) {
                                    if (!isExistFile(selectServerModel, perFile)) {
                                        remoteCopyFile(localServer, selectServerModel, perFile);

                                    }
                                }
                            }
                        }
                    }
                }

            }
        }
        System.out.println("恢复完成");
    }

    public boolean isExistFile(ServerInfoModel remoteServer, String fileName) {
        return true;
    }

    public void remoteCopyFile(ServerInfoModel sourceServer, ServerInfoModel remoteServer, String fileName) {

    }

    public List<String> getFiles() {
        return new ArrayList<String>();
    }

    private boolean needRecover(List<String> serverIds, int replicaPot) {
        boolean flag = false;
        for (int i = 1; i <= serverIds.size(); i++) {
            if (i != replicaPot) {
                if (getDeadMultiIds().contains(serverIds.get(i - 1))) {
                    flag = true;
                    break;
                }
            }
        }
        return flag;
    }

    private List<ServerInfoModel> getRecoverRoleList(String serverModel) {
        return new ArrayList<ServerInfoModel>();
    }

    private List<String> getDeadMultiIds() {
        return new ArrayList<String>();
    }

    private List<ServerInfoModel> getSelectedList(List<ServerInfoModel> aliveServerList, List<String> excludeServers) {
        List<ServerInfoModel> selectedList = new ArrayList<>();
        for (ServerInfoModel tmp : aliveServerList) {
            if (!excludeServers.contains(tmp.getMultiIdentification())) {
                selectedList.add(tmp);
            }
        }
        Collections.sort(selectedList, new CompareFromName());
        return selectedList;
    }

    public class CompareFromName implements Comparator<ServerInfoModel> {
        @Override
        public int compare(ServerInfoModel o1, ServerInfoModel o2) {
            return o1.getMultiIdentification().compareTo(o2.getMultiIdentification());
        }
    }

    public int hashFileName(String fileName, int size) {
        int nameSum = sumName(fileName);
        int matchSm = nameSum % size;
        return matchSm;
    }

    public int sumName(String name) {
        int sum = 0;
        for (int i = 0; i < name.length(); i++) {
            sum = sum + name.charAt(i);
        }
        return sum;
    }

    private boolean isAlive(String serverId) {
        if (getDeadMultiIds().contains(serverId)) {
            return false;
        } else {
            return true;
        }
    }

}
