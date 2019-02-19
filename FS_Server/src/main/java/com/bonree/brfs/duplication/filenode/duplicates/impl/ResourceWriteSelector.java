package com.bonree.brfs.duplication.filenode.duplicates.impl;

import com.bonree.brfs.duplication.filenode.duplicates.ClusterResource;
import com.bonree.brfs.duplication.filenode.duplicates.DuplicateNode;
import com.bonree.brfs.duplication.filenode.duplicates.DuplicateNodeSelector;
import com.bonree.brfs.duplication.filenode.duplicates.ServiceSelector;
import com.bonree.brfs.duplication.storageregion.StorageRegion;
import com.bonree.brfs.duplication.storageregion.StorageRegionManager;
import com.bonree.brfs.resourceschedule.model.ResourceModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Iterator;

public class ResourceWriteSelector implements DuplicateNodeSelector{
    private static final Logger LOG = LoggerFactory.getLogger(ResourceWriteSelector.class);
    private ClusterResource daemon = null;
    private ServiceSelector resourceSelector = null;
    private StorageRegionManager storageRegionManager = null;
    private String groupName = null;
    private ResourceWriteSelector(ClusterResource daemon, ServiceSelector resourceSelector, StorageRegionManager storageRegionManager,String groupName){
        this.daemon = daemon;
        this.resourceSelector = resourceSelector;
        this.storageRegionManager =storageRegionManager;
        this.groupName = groupName;

    }
    @Override
    public DuplicateNode[] getDuplicationNodes(int storageId, int nums){
        StorageRegion sr = this.storageRegionManager.findStorageRegionById(storageId);
        if(sr == null){
            LOG.error("srid : {} is not exist !!!",storageId);
            return new DuplicateNode[0];
        }
        // 获取资源信息
        Collection<ResourceModel> resources = daemon.getClusterResources();
        if(resources == null || resources.isEmpty()){
            LOG.error("[{}] select service list is empty !!!!", groupName);
            return new DuplicateNode[0];
        }
        // 过滤资源异常服务
        Collection<ResourceModel> selectors = this.resourceSelector.filterService(resources,sr.getName());
        if(selectors == null || selectors.isEmpty()){
            LOG.error("[{}] none avaible server to selector !!!",groupName);
            return new DuplicateNode[0];
        }
        // 按策略获取服务
        Collection<ResourceModel> wins = this.resourceSelector.selector(selectors,sr.getName(),nums);
        if(wins == null || wins.isEmpty()){
            LOG.error("[{}] no service can write !!!",groupName);
            return new DuplicateNode[0];
        }
        if(wins.size() != nums){
            LOG.warn("[{}] service can write !!!need {} but give {} ",groupName,nums,wins.size());
        }
        DuplicateNode[] duplicateNodes = new DuplicateNode[wins.size()];
        Iterator<ResourceModel> iterator = wins.iterator();
        int i = 0;
        while(iterator.hasNext()){
            duplicateNodes[i] = new DuplicateNode(groupName,iterator.next().getServerId());
            i++;
        }
        return duplicateNodes;
    }

    public static Builder newBuilder(){
        return new Builder();
    }
    public static class Builder{
        private ClusterResource daemon = null;
        private ServiceSelector resourceSelector = null;
        private StorageRegionManager storageRegionManager = null;
        private String groupName = null;
        public void setGroupName(String groupName){
            this.groupName = groupName;
        }

        private Builder(){

        }
        public void setDaemon(ClusterResource daemon){
            this.daemon = daemon;
        }

        public void setResourceSelector(ServiceSelector resourceSelector){
            this.resourceSelector = resourceSelector;
        }

        public void setStorageRegionManager(StorageRegionManager storageRegionManager){
            this.storageRegionManager = storageRegionManager;
        }

        public ResourceWriteSelector build(){
            return new ResourceWriteSelector(this.daemon,this.resourceSelector,this.storageRegionManager,this.groupName);
        }
    }
}
