package com.bonree.brfs.identification;

import com.bonree.brfs.rebalance.route.impl.RouteParserTest;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryNTimes;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * 版权信息: 北京博睿宏远数据科技股份有限公司
 * Copyright: Copyright (c) 2007-2020 北京博睿宏远数据科技股份有限公司,Inc.All Rights Reserved.
 *
 * @date: 2020年04月06日 18:25
 * @author: <a href=mailto:zhucg@bonree.com>朱成岗</a>
 * @description:
 **/
public class IDManagerTest {
    private static String ZKADDRES = RouteParserTest.ZK_ADDRESS;
    private static CuratorFramework client = null;

    @Before
    public void init() {
        client = CuratorFrameworkFactory.newClient(ZKADDRES, new RetryNTimes(5, 300));
        client.start();
        try {
            client.blockUntilConnected();
        } catch (InterruptedException e) {
            Assert.fail("zookeeper client is invaild !! address: " + ZKADDRES);
        }
    }

    @Test
    public void constructorTest() {

    }
}
