package com.bonree.brfs.partition;

import com.bonree.brfs.common.resource.vo.PartitionInfo;
import com.bonree.brfs.rebalance.route.impl.RouteParserTest;
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
 * @date 2020年03月24日 15:46:26
 * @author: <a href=mailto:zhucg@bonree.com>朱成岗</a>
 * @description:
 ******************************************************************************/

public class PartitionInfoRegisterTest {
    private static String ID_BAS_PATH = "/brfsDevTest/discovery";
    private static String ZKADDRES = RouteParserTest.ZK_ADDRESS;
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

    @Test
    public void registerPartitonInfoTest() throws Exception {
        PartitionInfoRegister manager = new PartitionInfoRegister(framework, ID_BAS_PATH);
        PartitionInfo obj = new PartitionInfo("dataGroup", "10", "diskGroup", "40", 100, 100, System.currentTimeMillis());
        manager.registerPartitionInfo(obj);
    }

    @Test
    public void unregisterPartitionInfoTest() throws Exception {
        PartitionInfoRegister manager = new PartitionInfoRegister(framework, ID_BAS_PATH);
        PartitionInfo obj = new PartitionInfo("dataGroup", "10", "diskGroup", "40", 100, 100, System.currentTimeMillis());
        manager.registerPartitionInfo(obj);
        Thread.sleep(1000);
        manager.unregisterPartitionInfo(obj);
        Thread.sleep(1000);
    }

    @After
    public void closeAll() {
        if (framework != null) {
            framework.close();
        }
    }
}
