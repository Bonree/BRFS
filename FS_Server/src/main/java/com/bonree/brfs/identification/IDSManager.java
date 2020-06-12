package com.bonree.brfs.identification;

import com.bonree.brfs.common.resource.vo.LocalPartitionInfo;
import com.bonree.brfs.common.service.Service;
import com.bonree.brfs.identification.impl.DiskDaemon;
import com.google.inject.Inject;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
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
public class IDSManager {
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
        start();
    }

    private Collection<String> convertToId(Collection<LocalPartitionInfo> partitions) {
        List<String> parts = new ArrayList<>();
        for (LocalPartitionInfo part : partitions) {
            parts.add(part.getPartitionId());
        }
        return parts;
    }

    public List<String> listValidVirtualIds(int storageIndex) {
        return this.virtualServerID.listValidVirtualIds(storageIndex);
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

    public boolean hasVirtual(String firstSever, String virtual, int storage) {
        return this.virtualServerID.hasVirtual(storage, virtual, firstSever);
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

    public Map<String, String> getSecondFirstShip(int storageRegionId) {
        return this.secondMaintainer.getSecondFirstRelationship(storageRegionId);
    }

    private void start() {
        Collection<LocalPartitionInfo> partitions = this.diskDaemon.getPartitions();
        Collection<String> parts = convertToId(partitions);
        this.secondMaintainer.addAllPartitionRelation(parts, firstSever);
        LOG.info("IDSManager start.");
    }

    public VirtualServerID getVirtualServerID() {
        return virtualServerID;
    }
}
