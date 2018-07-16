package com.bonree.brfs.authentication.impl;

import java.util.ArrayList;
import java.util.List;

import org.apache.curator.framework.CuratorFramework;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bonree.brfs.authentication.UserOperation;
import com.bonree.brfs.authentication.model.UserModel;
import com.bonree.brfs.common.utils.BrStringUtils;
import com.bonree.brfs.common.utils.JsonUtils;
import com.bonree.brfs.common.utils.JsonUtils.JsonException;
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

    private CuratorClient curatorClient;

    private final static String SEPARATOR = "/";

    private String basePath;

    public ZookeeperUserOperation(CuratorFramework client, String basePath) {
        this.curatorClient = CuratorClient.wrapClient(client);
        this.basePath = BrStringUtils.trimBasePath(basePath);
    }

    @Override
    public void createUser(UserModel user) {
    	try {
    		 String userNode = basePath + SEPARATOR + user.getUserName();
    	        String jsonStr = JsonUtils.toJsonString(user);
    	        if (!curatorClient.checkExists(userNode)) {
    	            curatorClient.createPersistent(userNode, false, jsonStr.getBytes());
    	        } else {
    	            LOG.warn("the user:" + user.getUserName() + " is exist!");
    	        }
		} catch (Exception e) {
			LOG.error("createUser", e);
		}
    }

    @Override
    public void deleteUser(String userName) {
        if (org.apache.commons.lang3.StringUtils.equals(userName, ROOT_USER)) {
            LOG.warn("can not delete: " + ROOT_USER);
            return;
        }
        String userNode = basePath + SEPARATOR + userName;
        curatorClient.delete(userNode, false);
    }

    @Override
    public void updateUser(UserModel user) {
    	try {
    		String userNode = basePath + SEPARATOR + user.getUserName();
            String jsonStr = JsonUtils.toJsonString(user);
            curatorClient.setData(userNode, jsonStr.getBytes());
		} catch (Exception e) {
			LOG.error("updateUser", e);
		}
    }

    @Override
    public UserModel getUser(String userName) {
        String userNode = basePath + SEPARATOR + userName;
        if (!curatorClient.checkExists(userNode)) {
            return null;
        }
        String jsonStr = new String(curatorClient.getData(userNode));
		try {
			return JsonUtils.toObject(jsonStr, UserModel.class);
		} catch (JsonException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        return null;
    }

    @Override
    public List<UserModel> getUserList() {
        List<UserModel> userList = new ArrayList<>();
        List<String> userNameList = curatorClient.getChildren(basePath);
        if (userNameList != null && userNameList.size() > 0) {
            for (String userName : userNameList) {
                UserModel user = getUser(userName);
                userList.add(user);
            }
        }
        return userList;
    }

}
