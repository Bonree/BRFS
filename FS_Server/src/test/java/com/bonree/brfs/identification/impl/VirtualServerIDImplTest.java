package com.bonree.brfs.identification.impl;

import com.bonree.brfs.rebalance.route.impl.RouteParserTest;
import java.util.Arrays;
import java.util.Collection;
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
 * @date 2020年04月02日 17:59:00
 * @author: <a href=mailto:zhucg@bonree.com>朱成岗</a>
 * @description: 了解虚拟serverid功能
 ******************************************************************************/

public class VirtualServerIDImplTest {
    private static String VIRTUAL_BAS_PATH = "/brfsDevTest";
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
    public void constructTest() {
        int storageId = 0;
        String firstServer = "10";
        String virtualId = "30";
        VirtualServerIDImpl virtualServerID = new VirtualServerIDImpl(framework, VIRTUAL_BAS_PATH);
        Collection<String> validIds = virtualServerID.listValidVirtualIds(storageId);
        System.out.println(validIds);
        virtualServerID.addFirstId(storageId, virtualId, firstServer);
        Collection<String> virtuals = virtualServerID.getVirtualID(storageId, 2, Arrays.asList(firstServer, "11", "12"));
        System.out.println(virtuals);
        boolean status = virtualServerID.invalidVirtualId(storageId, "30");
        //        boolean status = virtualServerID.validVirtualId(storageId,"30");
        System.out.println(status);

    }

    @After
    public void closeAll() {
        if (framework != null) {
            framework.close();
        }
    }
}
