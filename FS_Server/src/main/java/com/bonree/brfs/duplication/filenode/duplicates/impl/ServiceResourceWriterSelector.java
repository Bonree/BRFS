package com.bonree.brfs.duplication.filenode.duplicates.impl;

import com.bonree.brfs.common.service.ServiceManager;
import com.bonree.brfs.common.utils.Pair;
import com.bonree.brfs.duplication.datastream.connection.DiskNodeConnection;
import com.bonree.brfs.duplication.datastream.connection.DiskNodeConnectionPool;
import com.bonree.brfs.duplication.filenode.duplicates.ServiceSelector;
import com.bonree.brfs.resourceschedule.model.LimitServerResource;
import com.bonree.brfs.resourceschedule.model.ResourceModel;

import java.util.Collection;
import java.util.List;

/**
 * 服务写数据服务选择策略
 */
public class ServiceResourceWriterSelector implements ServiceSelector{
    private DiskNodeConnectionPool connectionPool = null;
    private ServiceManager serviceManager = null;
    private String groupName = null;
    private int centSize = 1000;
    private LimitServerResource limit = null;
    public ServiceResourceWriterSelector(DiskNodeConnectionPool connectionPool,ServiceManager serviceManager,LimitServerResource limit, String groupName, int centSize){
        this.connectionPool = connectionPool;
        this.serviceManager = serviceManager;
        this.groupName = groupName;
        this.centSize = centSize;
        this.limit = limit;
    }
    @Override
    public Collection<ResourceModel> filterService(Collection<ResourceModel> resourceModels, String path){
        return null;
    }

    @Override
    public Collection<ResourceModel> selector(Collection<ResourceModel> resources, String path, int num){
        return null;
    }

    @Override
    public List<Pair<String, Integer>> selectAvailableServers(int scene, String storageName, List<String> exceptionServerList, int centSize) throws Exception{
        return null;
    }

    @Override
    public List<Pair<String, Integer>> selectAvailableServers(int scene, int snId, List<String> exceptionServerList, int centSize) throws Exception{
        return null;
    }

    @Override
    public void setLimitParameter(LimitServerResource limits){
        this.limit = limits;
    }

    @Override
    public void update(ResourceModel resource){

    }

    @Override
    public void add(ResourceModel resources){

    }

    @Override
    public void remove(ResourceModel resource){

    }
}
