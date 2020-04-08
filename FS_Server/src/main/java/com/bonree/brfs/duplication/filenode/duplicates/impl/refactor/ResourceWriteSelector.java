package com.bonree.brfs.duplication.filenode.duplicates.impl.refactor;

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
    private ClusterResource daemon;
    private ServiceSelector resourceSelector;
    private String groupName;
    private DuplicateNodeSelector bakSelector;
    public ResourceWriteSelector(ClusterResource daemon, ServiceSelector resourceSelector,DuplicateNodeSelector bakSelector, String groupName){
        this.daemon = daemon;
        this.resourceSelector = resourceSelector;
        this.groupName = groupName;
        this.bakSelector = bakSelector;

    }
    @Override
    public DuplicateNode[] getDuplicationNodes(int storageId, int nums){
        long start = System.currentTimeMillis();
        DuplicateNode[] duplicateNodes;
        try{
            // 获取资源信息
            Collection<ResourceModel> resources = daemon.getClusterResources();
            // 采集资源未上传则使用备用选择器
            if(resources == null || resources.isEmpty()){
                LOG.warn("[{}] select resource list is empty !!!! use bak selector", groupName);
                return this.bakSelector.getDuplicationNodes(storageId,nums);
            }
            // 过滤资源异常服务
            Collection<ResourceModel> selectors = this.resourceSelector.filterService(resources,null);
            if(selectors == null || selectors.isEmpty()){
                LOG.error("[{}] none avaible server to selector !!!",groupName);
                return new DuplicateNode[0];
            }
            // 按策略获取服务
            Collection<ResourceModel> wins = this.resourceSelector.selector(selectors,null,nums);
            if(wins == null || wins.isEmpty()){
                LOG.error("[{}] no service can write !!!",groupName);
                return new DuplicateNode[0];
            }
            if(wins.size() != nums){
                LOG.warn("[{}] service can write !!!need {} but give {} ",groupName,nums,wins.size());
            }
            duplicateNodes = new DuplicateNode[wins.size()];
            Iterator<ResourceModel> iterator = wins.iterator();
            int i = 0;
            StringBuilder sBuild = new StringBuilder();
            sBuild.append("select service -> ");
            ResourceModel next;
            while(iterator.hasNext()){
                next = iterator.next();
                duplicateNodes[i] = new DuplicateNode(groupName, next.getServerId());
                i++;
                sBuild.append(i).append(":").append(next.getServerId()).append("(").append(next.getHost()).append(", remainSize").append(next.getDiskRemainRate()*next.getDiskRemainRate()).append("b ), ");
            }
            LOG.info("{}",sBuild.toString());


            return duplicateNodes;
        } finally{
            long stop = System.currentTimeMillis();
            LOG.info("resource select node time: {} ms",(stop - start));
        }
    }
}
