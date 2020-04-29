package com.bonree.brfs.resource;

import com.bonree.brfs.resource.vo.ResourceModel;

/**
 * 负责将资源信息注册到zk上
 */
public interface ResourceRegisterInterface {
    /**
     * 将资源信息注册到介质
     * @param model
     */
    void registerResource(ResourceModel model)throws Exception;
}
