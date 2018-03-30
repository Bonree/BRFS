package com.bonree.brfs.authentication;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.utils.ZKPaths;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bonree.brfs.authentication.impl.ZookeeperUserOperation;
import com.bonree.brfs.authentication.model.UserModel;
import com.bonree.brfs.common.utils.BrStringUtils;
import com.bonree.brfs.common.zookeeper.curator.cache.AbstractPathChildrenCacheListener;
import com.bonree.brfs.common.zookeeper.curator.cache.CuratorPathCache;

/*******************************************************************************
 * 版权信息：博睿宏远科技发展有限公司
 * Copyright: Copyright (c) 2007博睿宏远科技发展有限公司,Inc.All Rights Reserved.
 * 
 * @date 2018年3月19日 上午11:25:05
 * @Author: <a href=mailto:weizheng@bonree.com>魏征</a>
 * @Description: 简单认证策略,整个server需要一份即可，故为单例模式
 ******************************************************************************/
public class SimpleAuthentication implements UserOperation {

    private final static Logger LOG = LoggerFactory.getLogger(SimpleAuthentication.class);

    private final static String SEPARATOR_TOKEN = ":";

    public final static String LOCK_PATH = "/brfs/wz/locks/auth";

    private final Map<String, UserModel> userCache;

    private volatile static SimpleAuthentication auth = null;

    private UserOperation userOpt;

    private CuratorPathCache pathCache;
    private UserCacheListener userListener;

    class UserCacheListener extends AbstractPathChildrenCacheListener {
        public UserCacheListener(String listenName) {
            super(listenName);
        }

        @Override
        public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception {
            switch (event.getType()) {
                case CHILD_ADDED: {
                    InterProcessMutex lock = new InterProcessMutex(client, LOCK_PATH);
                    lock.acquire();
                    try {
                        LOG.info("add user");
                        String userName = ZKPaths.getNodeFromPath(event.getData().getPath());
                        UserModel user = userOpt.getUser(userName);
                        userCache.put(user.getUserName(), user);
                    } finally {
                        lock.release();
                    }
                    break;
                }
                case CHILD_REMOVED: {
                    InterProcessMutex lock = new InterProcessMutex(client, LOCK_PATH);
                    lock.acquire();
                    try {
                        LOG.info("delete user");
                        String userName = ZKPaths.getNodeFromPath(event.getData().getPath());
                        userCache.remove(userName);
                    } finally {
                        lock.release();
                    }
                    break;
                }
                case CHILD_UPDATED: {
                    InterProcessMutex lock = new InterProcessMutex(client, LOCK_PATH);
                    lock.acquire();
                    try {

                        LOG.info("update user");
                        String userName = ZKPaths.getNodeFromPath(event.getData().getPath());
                        UserModel user = userOpt.getUser(userName);
                        userCache.put(user.getUserName(), user);
                    } finally {
                        lock.release();
                    }
                    break;
                }
                default: {
                    LOG.info("other event");
                    break;
                }
            }
        }
    }

    private SimpleAuthentication(String basePath, String zkUrl) {
        String userPath = BrStringUtils.trimBasePath(basePath);
        userCache = new ConcurrentHashMap<String, UserModel>();
        userOpt = new ZookeeperUserOperation(zkUrl, userPath);
        List<UserModel> userList = userOpt.getUserList();
        for (UserModel user : userList) {
            userCache.put(user.getUserName(), user);
        }

        // 对节点进行监听
        pathCache = CuratorPathCache.getPathCacheInstance(zkUrl);
        userListener = new UserCacheListener("userManager");
        pathCache.addListener(userPath, userListener);
        pathCache.startPathCache(userPath);
        System.out.println(userPath);
    }

    /** 概述：初始化认证类
     * @param basePath zookeeper基本路径
     * @param zkUrl 用于连接zookeeper的地址
     * @return
     * @user <a href=mailto:weizheng@bonree.com>魏征</a>
     */
    public static SimpleAuthentication getAuthInstance(String basePath, String zkUrl) {
        LOG.info("init SimpleAuthentication...");
        if (auth == null) {
            synchronized (SimpleAuthentication.class) {
                if (auth == null) {
                    auth = new SimpleAuthentication(basePath, zkUrl);
                }

            }
        }
        return auth;
    }

    /** 概述：使用token进行验证,本次只验证用户名和密码，其他的权限验证暂时没做
     * @param token
     * @return
     * @user <a href=mailto:weizheng@bonree.com>魏征</a>
     */
    public boolean auth(String token) {
        String[] userPasswd = token.split(SEPARATOR_TOKEN);
        String userName = userPasswd[0];
        String passwd = userPasswd[1];
        UserModel user = userCache.get(userName);
        if (user != null) {
            if (org.apache.commons.lang3.StringUtils.equals(passwd, user.getPasswd())) {
                return true;
            }
        }
        return false;
    }

    @Override
    public void createUser(UserModel user) {
        userOpt.createUser(user);
    }

    @Override
    public void deleteUser(String userName) {
        userOpt.deleteUser(userName);

    }

    @Override
    public void updateUser(UserModel user) {
        userOpt.updateUser(user);

    }

    @Override
    public UserModel getUser(String userName) {
        return userOpt.getUser(userName);
    }

    @Override
    public List<UserModel> getUserList() {
        return userOpt.getUserList();
    }

}
