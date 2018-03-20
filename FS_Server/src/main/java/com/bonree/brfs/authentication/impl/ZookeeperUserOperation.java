package com.bonree.brfs.authentication.impl;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    private final static Logger LOG = LoggerFactory.getLogger(ZookeeperUserOperation.class);

    private final static String ROOT_USER = "root";

    private String zkUrl;

    private final static String SEPARATOR = "/";

    private String basePath;

    public ZookeeperUserOperation(String zkUrl, String basePath) {
        this.zkUrl = zkUrl;
        this.basePath = StringUtils.trimBasePath(basePath);
    }

    @Override
    public void createUser(UserModel user) {
        CuratorClient client = CuratorClient.getClientInstance(zkUrl);
        try {
            String userNode = basePath + SEPARATOR + user.getUserName();
            String jsonStr = JSON.toJSONString(user);
            if (!client.checkExists(userNode)) {
                client.createPersistent(userNode, false, jsonStr.getBytes());
            } else {
                LOG.warn("the user:" + user.getUserName() + " is exist!");
            }
        } finally {
            client.close();
        }
    }

    @Override
    public void deleteUser(String userName) {
        if (org.apache.commons.lang3.StringUtils.equals(userName, ROOT_USER)) {
            LOG.warn("can not delete: " + ROOT_USER);
            return;
        }
        CuratorClient client = CuratorClient.getClientInstance(zkUrl);
        try {
            String userNode = basePath + SEPARATOR + userName;
            client.delete(userNode, false);
        } finally {
            client.close();
        }
    }

    @Override
    public void updateUser(UserModel user) {
        CuratorClient client = CuratorClient.getClientInstance(zkUrl);
        try {
            String userNode = basePath + SEPARATOR + user.getUserName();
            String jsonStr = JSON.toJSONString(user);
            client.setData(userNode, jsonStr.getBytes());
        } finally {
            client.close();
        }
    }

    @Override
    public UserModel getUser(String userName) {
        CuratorClient client = CuratorClient.getClientInstance(zkUrl);
        try {
            String userNode = basePath + SEPARATOR + userName;
            String jsonStr = new String(client.getData(userNode));
            UserModel user = JSON.parseObject(jsonStr, UserModel.class);
            return user;
        } finally {
            client.close();
        }
    }

    @Override
    public List<UserModel> getUserList() {
        CuratorClient client = CuratorClient.getClientInstance(zkUrl);
        try {
            List<UserModel> userList = new ArrayList<>();
            List<String> userNameList = client.getChildren(basePath);
            if (userNameList != null && userNameList.size() > 0) {
                for (String userName : userNameList) {
                    UserModel user = getUser(userName);
                    userList.add(user);
                }
            }
            return userList;
        } finally {
            client.close();
        }
    }

}
