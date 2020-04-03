package com.bonree.brfs.identification;

import java.util.Collection;
import java.util.List;

/**
 * 版权信息: 北京博睿宏远数据科技股份有限公司
 * Copyright: Copyright (c) 2007-2020 北京博睿宏远数据科技股份有限公司,Inc.All Rights Reserved.
 *
 * @date: 2020年04月03日 15:53
 * @author: <a href=mailto:zhucg@bonree.com>朱成岗</a>
 * @description: id综合查询管理类，负责二级serverid，虚拟serverid 的查询，以及datanode一级server查询
 **/
public class IDSManager implements SecondMaintainerInterface,VirtualServerID {
    private SecondMaintainerInterface secondMaintainer;
    private VirtualServerID virtualServerID;


    @Override
    public String registerSecondId(String firstServer, String partitionId, int storageId) {
        return null;
    }

    @Override
    public Collection<String> registerSecondIds(String firstServer, int storageId) {
        return null;
    }

    @Override
    public boolean unregisterSecondId(String firstServer, String partitionId, int storageId) {
        return false;
    }

    @Override
    public boolean unregisterSecondIds(String firstServer, int storageid) {
        return false;
    }

    @Override
    public boolean isValidSecondId(String secondId, int storageId) {
        return false;
    }

    @Override
    public void addPartitionRelation(String firstServer, String partitionId) {

    }

    @Override
    public void addAllPartitionRelation(Collection<String> parititionId, String firstServer) {

    }

    @Override
    public boolean removePartitionRelation(String partitionid) {
        return false;
    }

    @Override
    public boolean removeAllPartitionRelation(Collection<String> partitionIds) {
        return false;
    }

    @Override
    public List<String> getVirtualID(int storageIndex, int count, List<String> diskFirstIDs) {
        return null;
    }

    @Override
    public List<String> listValidVirtualIds(int storageIndex) {
        return null;
    }

    @Override
    public List<String> listInvalidVirtualIds(int storageIndex) {
        return null;
    }

    @Override
    public boolean invalidVirtualId(int storageIndex, String id) {
        return false;
    }

    @Override
    public boolean validVirtualId(int storageIndex, String id) {
        return false;
    }

    @Override
    public boolean deleteVirtualId(int storageIndex, String id) {
        return false;
    }

    @Override
    public String getVirtualIdContainerPath() {
        return null;
    }

    @Override
    public void addFirstId(int storageIndex, String virtualID, String firstId) {

    }
}
