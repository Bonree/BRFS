package com.bonree.brfs.identification.impl;

import com.bonree.brfs.common.zookeeper.curator.cache.CuratorCacheFactory;
import com.bonree.brfs.identification.SecondIdsInterface;
import com.bonree.brfs.rebalance.route.impl.RouteParserTest;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryNTimes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.Collection;

/*******************************************************************************
 * 版权信息： 北京博睿宏远数据科技股份有限公司
 * Copyright (c) 2007-2020 北京博睿宏远数据科技股份有限公司，Inc. All Rights Reserved.
 * @date 2020年04月01日 10:25:09
 * @author: <a href=mailto:zhucg@bonree.com>朱成岗</a>
 * @description:
 ******************************************************************************/

public class SecondIDRelationShipTest {
    private static String ZKADDRES = RouteParserTest.ZK_ADDRESS;
    private static final String SBAS_PATH="/brfsDevTest/service_ids";
    private CuratorFramework client = null;
    private SecondIDRelationShip ship = null;

    /**
     * 初始化zk连接及目录书
     */
    @Before
    public void init()throws Exception{
        client = CuratorFrameworkFactory.newClient(ZKADDRES, new RetryNTimes(5,100));
        client.start();
        client.blockUntilConnected();
        // 创建测试根目录
        if(client.checkExists().forPath(SBAS_PATH) == null){
            client.create().forPath(SBAS_PATH);
        }
        // 实例化TreeCacheFactory
        CuratorCacheFactory.init(client);
        ship = new SecondIDRelationShip(client,SBAS_PATH);
        ship.start();
    }
    public void createPartition(String firstServer,String partitionId)throws Exception{
        String pPath = SBAS_PATH+"/"+partitionId;
        if(client.checkExists().forPath(pPath) !=null){
            client.setData().forPath(pPath,firstServer.getBytes(StandardCharsets.UTF_8));
        }else{
            client.create().creatingParentsIfNeeded().forPath(pPath,firstServer.getBytes(StandardCharsets.UTF_8));
        }
    }
    public void createSecondIdWithAll(String firstServer,String partitionId,String storageId,String secondId)throws Exception{
        createPartition(firstServer,partitionId);
        createSecondIdOnly(partitionId,storageId,secondId);

    }
    public void createSecondIdOnly(String partitionId,String storageId,String secondId) throws Exception{
        String sPath = SBAS_PATH+"/"+partitionId+"/"+storageId;
        if(client.checkExists().forPath(sPath) !=null){
            client.setData().forPath(sPath,secondId.getBytes(StandardCharsets.UTF_8));
        }else{
            client.create().creatingParentsIfNeeded().forPath(sPath,secondId.getBytes(StandardCharsets.UTF_8));
        }
    }
    @Test
    public void addPartitionTest()throws Exception{
        String firstServer = "10";
        String partitionId = "40";
        createPartition(firstServer,partitionId);
    }
    @Test
    public void addSecondPartitionTest() throws Exception{
        String firstServer = "10";
        String partitionId = "40";
        String storageId = "0";
        String secondId = "20";
        createSecondIdWithAll(firstServer,partitionId,storageId,secondId);
        String tmpSecond = ship.getSecondId(partitionId,Integer.parseInt(storageId));
        assert secondId.equals(tmpSecond);
        String tmpServer = ship.getFirstId(secondId,Integer.parseInt(storageId));
        assert firstServer.equals(tmpServer);
        Collection<String> services = ship.getSecondIds(firstServer,Integer.parseInt(storageId));
        System.out.println(services);
        assert services.contains(secondId);
    }

    @After
    public void close()throws Exception{
//        if(client !=null && client.checkExists().forPath(SBAS_PATH) !=null){
//            client.delete().forPath(SBAS_PATH);
//        }
        client.close();
    }
}
