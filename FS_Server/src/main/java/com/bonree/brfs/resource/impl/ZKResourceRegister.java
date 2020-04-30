package com.bonree.brfs.resource.impl;

import com.bonree.brfs.common.service.Service;
import com.bonree.brfs.common.utils.JsonUtils;
import com.bonree.brfs.resource.ResourceRegisterInterface;
import com.bonree.brfs.resource.vo.ResourceModel;
import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.CreateMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 资源信息注册
 */
public class ZKResourceRegister implements ResourceRegisterInterface {
    private static final Logger LOG = LoggerFactory.getLogger(ZKResourceRegister.class);
    private CuratorFramework client = null;
    private Service localService;
    private String registerPath;

    public ZKResourceRegister(CuratorFramework client, Service localService, String basePath) {
        this.client = client;
        this.localService = localService;
        this.registerPath = basePath + "/" + localService.getServiceId();
    }

    @Override
    public void registerResource(ResourceModel model) throws Exception {
        byte[] data = JsonUtils.toJsonBytesQuietly(model);
        if (data == null || data.length == 0) {
            throw new NullPointerException(localService.getServiceId() + " gather resource is empty !!");
        }
        if (client.checkExists().forPath(registerPath) == null) {
            client.create().withMode(CreateMode.EPHEMERAL).forPath(registerPath, data);
        } else {
            client.setData().forPath(registerPath, data);
        }
    }
}
