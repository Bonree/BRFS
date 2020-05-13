package com.bonree.brfs.identification;

import com.bonree.brfs.common.process.LifeCycle;
import com.bonree.brfs.common.service.Service;
import com.bonree.brfs.identification.impl.DiskDaemon;
import com.bonree.brfs.partition.model.LocalPartitionInfo;
import com.google.inject.Inject;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 版权信息: 北京博睿宏远数据科技股份有限公司
 * Copyright: Copyright (c) 2007-2020 北京博睿宏远数据科技股份有限公司,Inc.All Rights Reserved.
 *
 * @date: 2020年04月03日 15:53
 * @author: <a href=mailto:zhucg@bonree.com>朱成岗</a>
 * @description: id综合查询管理类，负责二级serverid，虚拟serverid 的查询，以及datanode一级server查询
 **/
public class IDSManager implements LifeCycle {
    private static final Logger LOG = LoggerFactory.getLogger(IDSManager.class);
    private String firstSever = null;
    private SecondMaintainerInterface secondMaintainer;
    private VirtualServerID virtualServerID;
    private DiskDaemon diskDaemon;

    @Inject
    public IDSManager(Service localService, SecondMaintainerInterface secondMaintainer, VirtualServerID virtualServerID,
                      DiskDaemon diskDaemon) {
        this.firstSever = localService.getServiceId();
        this.secondMaintainer = secondMaintainer;
        this.virtualServerID = virtualServerID;
        this.diskDaemon = diskDaemon;
    }

    private Collection<String> convertToId(Collection<LocalPartitionInfo> partitions) {
        List<String> parts = new ArrayList<>();
        for (LocalPartitionInfo part : partitions) {
            parts.add(part.getPartitionId());
        }
        return parts;
    }

    /**
     * 注册二级serverid
     *
     * @param firstServer
     * @param partitionId
     * @param storageId
     *
     * @return
     */
    public String registerSecondId(String firstServer, String partitionId, int storageId) {
        return this.secondMaintainer.registerSecondId(firstServer, partitionId, storageId);
    }

    /**
     * 注册二级serverid
     *
     * @param firstServer
     * @param storageId
     *
     * @return
     */
    public Collection<String> registerSecondIds(String firstServer, int storageId) {
        return this.secondMaintainer.registerSecondIds(firstServer, storageId);
    }

    public boolean unregisterSecondId(String partitionId, int storageId) {
        return this.secondMaintainer.unregisterSecondId(partitionId, storageId);
    }

    public boolean unregisterSecondIds(String firstServer, int storageid) {
        return this.secondMaintainer.unregisterSecondId(firstServer, storageid);
    }

    public boolean isValidSecondId(String secondId, int storageId) {
        return this.secondMaintainer.isValidSecondId(secondId, storageId);
    }

    public void addPartitionRelation(String firstServer, String partitionId) {
        this.secondMaintainer.addPartitionRelation(firstServer, partitionId);
    }

    public void addAllPartitionRelation(Collection<String> parititionId, String firstServer) {
        this.secondMaintainer.addAllPartitionRelation(parititionId, firstServer);
    }

    public boolean removePartitionRelation(String partitionid) {
        return this.secondMaintainer.removePartitionRelation(partitionid);
    }

    public boolean removeAllPartitionRelation(Collection<String> partitionIds) {
        return this.secondMaintainer.removeAllPartitionRelation(partitionIds);
    }

    public List<String> getVirtualID(int storageIndex, int count, List<String> diskFirstIDs) {
        return this.virtualServerID.getVirtualID(storageIndex, count, diskFirstIDs);
    }

    public List<String> listValidVirtualIds(int storageIndex) {
        return this.virtualServerID.listValidVirtualIds(storageIndex);
    }

    public List<String> listInvalidVirtualIds(int storageIndex) {
        return this.virtualServerID.listInvalidVirtualIds(storageIndex);
    }

    public boolean invalidVirtualId(int storageIndex, String id) {
        return this.virtualServerID.invalidVirtualId(storageIndex, id);
    }

    public boolean validVirtualId(int storageIndex, String id) {
        return this.virtualServerID.validVirtualId(storageIndex, id);
    }

    public boolean deleteVirtualId(int storageIndex, String id) {
        return this.virtualServerID.deleteVirtualId(storageIndex, id);
    }

    public String getVirtualIdContainerPath() {
        return this.virtualServerID.getVirtualIdContainerPath();
    }

    public void addFirstId(int storageIndex, String virtualID, String firstId) {
        this.virtualServerID.addFirstId(storageIndex, virtualID, firstId);
    }

    public String getFirstSever() {
        return firstSever;
    }

    public Collection<String> getSecondIds(String serverId, int storageRegionId) {
        return this.secondMaintainer.getSecondIds(serverId, storageRegionId);
    }

    public String getSecondId(String partitionId, int storageRegionId) {
        return this.secondMaintainer.getSecondId(partitionId, storageRegionId);
    }

    public String getFirstId(String secondId, int storageRegionId) {
        return this.secondMaintainer.getFirstId(secondId, storageRegionId);
    }

    public String getPartitionId(String secondId, int storageRegionId) {
        return this.secondMaintainer.getPartitionId(secondId, storageRegionId);
    }

    
    @Override
    public void start() throws Exception {
        Collection<LocalPartitionInfo> partitions = this.diskDaemon.getPartitions();
        Collection<String> parts = convertToId(partitions);
        this.secondMaintainer.addAllPartitionRelation(parts, firstSever);
        LOG.info("IDSManager start.");
    }

    @Override
    public void stop() throws Exception {

    }
}
