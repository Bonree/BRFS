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
 * @date 2020年04月01日 16:28:44
 * @author: <a href=mailto:zhucg@bonree.com>朱成岗</a>
 * @description:
 ******************************************************************************/

public class SimpleSecondMaintainerTest {
    private static String ZKADDRES = RouteParserTest.ZK_ADDRESS;
    private static String S_B_PATH = "/brfsDevTest/service_ids";
    private static String S_SEQ_PATH = "/brfsDevTest/second_seq";
    private static String ROUTE_PATH = "/brfsDevTest/route/V2";
    private CuratorFramework client;
    @Before
    public void init(){
        client = CuratorFrameworkFactory.newClient(ZKADDRES,new RetryNTimes(5,300));
        client.start();
        try {
            client.blockUntilConnected();
        } catch (InterruptedException e) {
            Assert.fail("zookeeper client is invaild !! address: "+ZKADDRES);
        }
    }
    @Test
    public void constructorTest(){
        String firstServer = "10";
        SimpleSecondMaintainer maintainer = new SimpleSecondMaintainer(client,S_B_PATH,ROUTE_PATH,S_SEQ_PATH,firstServer);
    }
    @Test
    public void registerSecondIdTest(){
        String firstServer = "10";
        String partitionId = "40";
        int storageId = 0;
        String expectSecond = "20";
        SimpleSecondMaintainer maintainer = new SimpleSecondMaintainer(client,S_B_PATH,ROUTE_PATH,S_SEQ_PATH,firstServer);
        String secondId = maintainer.registerSecondId(firstServer,partitionId,storageId);
        Assert.assertEquals(expectSecond,secondId);
    }
    @Test
    public void unregisterSecondIdTest(){
        String firstServer = "10";
        String partitionId = "41";
        int storageId = 0;
        SimpleSecondMaintainer maintainer = new SimpleSecondMaintainer(client,S_B_PATH,ROUTE_PATH,S_SEQ_PATH,firstServer);
        String expectSecond = maintainer.registerSecondId(firstServer,partitionId,storageId);
        System.out.println(expectSecond);
        boolean status = maintainer.unregisterSecondId(partitionId,storageId);
        Assert.assertEquals(true,status);
    }
    @Test
    public void registerSecondIdBatch(){
        String firstServer = "10";
        int storageId = 1;
        SimpleSecondMaintainer maintainer = new SimpleSecondMaintainer(client,S_B_PATH,ROUTE_PATH,S_SEQ_PATH,firstServer);
        maintainer.registerSecondIds(firstServer,storageId);
    }
    @Test
    public void unregisterSecondIdBatch(){
        String firstServer = "10";
        int storageId = 1;
        SimpleSecondMaintainer maintainer = new SimpleSecondMaintainer(client,S_B_PATH,ROUTE_PATH,S_SEQ_PATH,firstServer);
        maintainer.registerSecondIds(firstServer,storageId);
        maintainer.unregisterSecondIds(firstServer,storageId);
    }
    @Test
    public void addPartitionRelationshipTest(){
        String firstServer = "10";
        int storageId = 1;
        String partitionid = "45";
        SimpleSecondMaintainer maintainer = new SimpleSecondMaintainer(client,S_B_PATH,ROUTE_PATH,S_SEQ_PATH,firstServer);
        maintainer.addPartitionRelation(firstServer,partitionid);
    }

    @Test
    public void removePartitionRelationshipTest(){
        String firstServer = "10";
        int storageId = 1;
        String partitionid = "45";
        SimpleSecondMaintainer maintainer = new SimpleSecondMaintainer(client,S_B_PATH,ROUTE_PATH,S_SEQ_PATH,firstServer);
        maintainer.removePartitionRelation(partitionid);
    }
    @After
    public void closeAll(){
        if(client != null){
            client.close();
        }
    }
}
