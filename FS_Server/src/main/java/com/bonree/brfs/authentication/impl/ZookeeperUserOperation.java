package com.bonree.brfs.authentication.impl;


import com.alibaba.fastjson.JSON;
import com.bonree.brfs.authentication.UserOperation;
import com.bonree.brfs.authentication.model.UserModel;
import com.bonree.brfs.common.utils.StringUtils;
import com.bonree.brfs.common.zookeeper.curator.CuratorClient;

/*******************************************************************************
 * 版权信息：博睿宏远科技发展有限公司
 * Copyright: Copyright (c) 2007博睿宏远科技发展有限公司,Inc.All Rights Reserved.
 * 
 * @date 2018年3月19日 上午11:25:10
 * @Author: <a href=mailto:weizheng@bonree.com>魏征</a>
 * @Description: 操作User，如添加，删除，更改
 ******************************************************************************/
public class ZookeeperUserOperation implements UserOperation {

    private CuratorClient client;

    private String basePath;

    public ZookeeperUserOperation(CuratorClient client, String basePath) {
        this.client = client;
        this.basePath = StringUtils.normalBasePath(basePath);
    }

    @Override
    public void createUser(UserModel user) {
        String userNode = basePath + user.getUserName();
        String jsonStr = JSON.toJSONString(user);
        client.createPersistent(userNode, false, jsonStr.getBytes());
    }

    @Override
    public void deleteUser(UserModel user) {
        String userNode = basePath + user.getUserName();
        client.delete(userNode, false);
    }

    @Override
    public void updateUser(UserModel user) {
        String userNode = basePath + user.getUserName();
        String jsonStr = JSON.toJSONString(user);
        client.setData(userNode, jsonStr.getBytes());
    }

    @Override
    public UserModel getUser(String userName) {
        String userNode = basePath + userName;
        String jsonStr = new String(client.getData(userNode));
        UserModel user = JSON.parseObject(jsonStr, UserModel.class);
        return user;
    }
}
