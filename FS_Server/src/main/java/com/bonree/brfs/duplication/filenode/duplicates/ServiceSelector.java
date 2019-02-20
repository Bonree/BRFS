package com.bonree.brfs.duplication.filenode.duplicates;

import com.bonree.brfs.resourceschedule.model.ResourceModel;
import com.bonree.brfs.resourceschedule.service.ResourceSelector;

import java.util.Collection;

public interface ServiceSelector extends ResourceSelector{
    /**
     * 过滤限制的服务的serviceids
     */
    Collection<ResourceModel> filterService(Collection<ResourceModel> resourceModels,String path);

    /**
     * 选择服务
     * @param resources
     * @param path
     * @param num
     * @return
     */
    Collection<ResourceModel> selector(Collection<ResourceModel> resources,String path,int num);
}
