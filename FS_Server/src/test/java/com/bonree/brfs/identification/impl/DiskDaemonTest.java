package com.bonree.brfs.identification.impl;

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
 * @date 2020年03月24日 11:55:12
 * @author: <a href=mailto:zhucg@bonree.com>朱成岗</a>
 * @description:
 ******************************************************************************/

public class DiskDaemonTest {
    private static String FILE_DIR = DiskDaemonTest.class.getResource("/DiskNodeId").getPath();
    private static String ID_BAS_PATH="/brfsDevTest/identification/server_ids";
    private static String ID_SEQ_PATH="/brfsDevTest/identification/server_id_sequences";
    private static String ZKADDRES = RouteParserTest.ZK_ADDRESS;
    private static String DISK_NODE_NAME="DiskNodeIDs";
    private CuratorFramework framework = null;
    @Before
    public void checkZK(){
        framework = CuratorFrameworkFactory.newClient(ZKADDRES,new RetryNTimes(5,300));
        framework.start();
        try {
            framework.blockUntilConnected();
        } catch (InterruptedException e) {
            Assert.fail("zookeeper client is invaild !! address: "+ZKADDRES);
        }
    }
    @Test
    public void initTest(){
    }
    @After
    public void closeAll(){
        if(framework != null){
            framework.close();
        }
    }
}
