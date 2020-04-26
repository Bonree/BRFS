package com.bonree.brfs.identification.impl;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryNTimes;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/*******************************************************************************
 * 版权信息： 北京博睿宏远数据科技股份有限公司
 * Copyright (c) 2007-2020 北京博睿宏远数据科技股份有限公司，Inc. All Rights Reserved.
 * @date 2020年03月30日 15:34:32
 * @author: <a href=mailto:zhucg@bonree.com>朱成岗</a>
 * @description:
 ******************************************************************************/
public class DiskNodeIDImplTest {
    private static String ID_BAS_PATH = "/brfs/data1/disk";
    private static String ID_SECOND_PATH = "/brfs/data1/secondIDSet";
    private static String ZKADDRES = "192.168.150.236:2181";
    private CuratorFramework framework = null;

    @Before
    public void checkZK() {
        framework = CuratorFrameworkFactory.newClient(ZKADDRES, new RetryNTimes(5, 300));
        framework.start();
        try {
            framework.blockUntilConnected();
        } catch (InterruptedException e) {
            Assert.fail("zookeeper client is invaild !! address: " + ZKADDRES);
        }
    }

    /**
     * TestCase 1:  测试构建实例是否报错
     */
    @Test
    public void newConstructorTest() {
        DiskNodeIDImpl impl = new DiskNodeIDImpl(framework, ID_BAS_PATH, ID_SECOND_PATH);
    }

    /**
     * TestCase 2: 测试获取磁盘节点唯一id
     */
    @Test
    public void getLevelTest() {
        DiskNodeIDImpl impl = new DiskNodeIDImpl(framework, ID_BAS_PATH, ID_SECOND_PATH);
        try {
            if (framework.checkExists().forPath(ID_BAS_PATH) != null) {
                framework.delete().deletingChildrenIfNeeded().forPath(ID_BAS_PATH);
            }
            if (framework.checkExists().forPath(ID_SECOND_PATH + "/40") == null) {
                framework.create().creatingParentsIfNeeded().forPath(ID_SECOND_PATH + "/40");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.out.println(impl.genLevelID());

    }

    @After
    public void closeAll() {
        if (framework != null) {
            framework.close();
        }
    }

}
