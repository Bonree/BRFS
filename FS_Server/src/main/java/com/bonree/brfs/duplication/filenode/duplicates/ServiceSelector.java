package com.bonree.brfs.duplication.filenode.duplicates;

import com.bonree.brfs.resource.vo.ResourceModel;
import java.util.Collection;

public interface ServiceSelector {

    /**
     * 从 resources 中选择n个节点放入result中
     * @return
     */
    public Collection<ResourceModel> selector(Collection<ResourceModel> resources, int n);
}
