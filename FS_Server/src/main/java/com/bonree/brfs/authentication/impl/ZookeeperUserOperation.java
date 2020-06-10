package com.bonree.brfs.authentication.impl;

import com.bonree.brfs.authentication.UserOperation;
import com.bonree.brfs.authentication.model.UserModel;
import com.bonree.brfs.common.utils.BrStringUtils;
import com.bonree.brfs.common.utils.JsonUtils;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.utils.ZKPaths;
import org.apache.zookeeper.CreateMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*******************************************************************************
 * 版权信息：博睿宏远科技发展有限公司
 * Copyright: Copyright (c) 2007博睿宏远科技发展有限公司,Inc.All Rights Reserved.
 *
 * @date 2018年3月19日 上午11:25:10
 * @Author: <a href=mailto:weizheng@bonree.com>魏征</a>
 * @Description: 操作User，如添加，删除，更改
 ******************************************************************************/
public class ZookeeperUserOperation implements UserOperation {

    private static final Logger LOG = LoggerFactory.getLogger(ZookeeperUserOperation.class);

    private static final String ROOT_USER = "root";

    private CuratorFramework client;

    private String basePath;

    public ZookeeperUserOperation(CuratorFramework client, String basePath) {
        this.client = client;
        this.basePath = BrStringUtils.trimBasePath(basePath);
    }

    @Override
    public void createUser(UserModel user) {
        try {
            String userNode = ZKPaths.makePath(basePath, user.getUserName());
            String jsonStr = JsonUtils.toJsonString(user);
            if (client.checkExists().forPath(userNode) == null) {
                client.create()
                      .creatingParentsIfNeeded()
                      .withMode(CreateMode.PERSISTENT)
                      .forPath(userNode, jsonStr.getBytes(StandardCharsets.UTF_8));
            } else {
                LOG.warn("the user:" + user.getUserName() + " is exist!");
            }
        } catch (Exception e) {
            LOG.error("createUser", e);
        }
    }

    @Override
    public void deleteUser(String userName) {
        try {
            if (org.apache.commons.lang3.StringUtils.equals(userName, ROOT_USER)) {
                LOG.warn("can not delete: " + ROOT_USER);
                return;
            }
            String userNode = ZKPaths.makePath(basePath, userName);
            client.delete().forPath(userNode);
        } catch (Exception e) {
            LOG.error("deleteUser [{}] happen error", userName, e);
        }

    }

    @Override
    public void updateUser(UserModel user) {
        try {
            String userNode = ZKPaths.makePath(basePath, user.getUserName());
            String jsonStr = JsonUtils.toJsonString(user);
            client.setData().forPath(userNode, jsonStr.getBytes(StandardCharsets.UTF_8));
        } catch (Exception e) {
            LOG.error("updateUser", e);
        }
    }

    @Override
    public UserModel getUser(String userName) {
        String userNode = ZKPaths.makePath(basePath, userName);
        try {
            if (client.checkExists().forPath(userNode) == null) {
                return null;
            }
            byte[] data = client.getData().forPath(userNode);
            String jsonStr = new String(data, StandardCharsets.UTF_8);
            return JsonUtils.toObject(jsonStr, UserModel.class);
        } catch (Exception e) {
            LOG.error("getUser [{}] happen error ", userName, e);
        }
        return null;
    }

    @Override
    public List<UserModel> getUserList() {
        List<UserModel> userList = new ArrayList<>();
        List<String> userNameList = null;
        try {
            userNameList = client.getChildren().forPath(basePath);
        } catch (Exception e) {
            LOG.error("getUserList happen error ", e);
        }
        if (userNameList != null && userNameList.size() > 0) {
            for (String userName : userNameList) {
                UserModel user = getUser(userName);
                userList.add(user);
            }
        }
        return userList;
    }

}
