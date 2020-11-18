package com.bonree.brfs.identification.impl;

import com.bonree.brfs.common.ZookeeperPaths;
import java.util.List;
import javax.validation.constraints.AssertTrue;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryNTimes;
import org.apache.curator.utils.ZKPaths;
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
    private static String ZKADDRES = "localhost:2181";
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
        ZookeeperPaths zkPaths = ZookeeperPaths.getBasePath("brfs7", framework);
        DiskNodeIDImpl impl = new DiskNodeIDImpl(framework, zkPaths);
    }

    /**
     * TestCase 2: 测试获取磁盘节点唯一id
     */
    @Test
    public void getLevelTest() {
        ZookeeperPaths zkPaths = ZookeeperPaths.getBasePath("data1", framework);
        DiskNodeIDImpl impl = new DiskNodeIDImpl(framework, zkPaths);
        try {
            List<String> children = framework.getChildren().forPath(zkPaths.getBaseV2SecondIDPath());
            int theLastPartition = 0;
            int curPartition;
            for (String child : children) {
                curPartition = Integer.parseInt(child);
                if (Integer.parseInt(child) > theLastPartition) {
                    theLastPartition = curPartition;
                }
            }
            String nextID = impl.genLevelID();
            framework.create().forPath(ZKPaths.makePath(zkPaths.getBaseV2SecondIDPath(), nextID));
            nextID = nextID.substring(1, nextID.length() - 2);
            String idWithLevel = String.valueOf(theLastPartition);
            idWithLevel = idWithLevel.substring(1, idWithLevel.length() - 2);
            int expectID = Integer.parseInt(idWithLevel);
            Assert.assertEquals(theLastPartition + 1, Integer.parseInt(nextID));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @After
    public void closeAll() {
        if (framework != null) {
            framework.close();
        }
    }

}
