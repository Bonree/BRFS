package com.bonree.brfs.identification.impl;

import com.bonree.brfs.common.service.Service;
import com.bonree.brfs.partition.PartitionCheckingRoutine;
import com.bonree.brfs.partition.PartitionGather;
import com.bonree.brfs.partition.PartitionInfoRegister;
import com.bonree.brfs.partition.model.LocalPartitionInfo;
import com.bonree.brfs.rebalance.route.impl.RouteParserTest;
import com.bonree.brfs.resourceschedule.utils.LibUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryNTimes;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.Collection;

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
    private CuratorFramework client = null;
    @Before
    public void checkZK(){
        client = CuratorFrameworkFactory.newClient(ZKADDRES,new RetryNTimes(5,300));
        client.start();
        try {
            client.blockUntilConnected();
        } catch (InterruptedException e) {
            Assert.fail("zookeeper client is invaild !! address: "+ZKADDRES);
        }
        String libPath = "E:\\worker\\Bonree\\BrfsSecond\\BRFS\\lib";
        try {
            LibUtils.loadLibraryPath(libPath);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    @Test
    public void initTest(){
        String partitionSeqPath = ID_SEQ_PATH;
        String path = "D:/tmp";
        String rootPath = path+"/data";
        String innerPath = path+"/partitionIds";
        String partitionGroup = "partition";
        Service firstServer = new Service("13","zhucgTest","192.168.4.13",20000);
        String partitionGroupBasepath = "/brfs/DevTest/disconvery";
        // 1.生成注册id实例
        DiskNodeIDImpl diskNodeID = new DiskNodeIDImpl(client,partitionSeqPath);
        // 2.生成磁盘分区id检查类
        PartitionCheckingRoutine routine = new PartitionCheckingRoutine(diskNodeID,rootPath,innerPath,partitionGroup);
        Collection<LocalPartitionInfo> parts = routine.checkVaildPartition();
        // 3.生成注册管理实例
        PartitionInfoRegister register = new PartitionInfoRegister(client,partitionGroupBasepath);
        // 4.生成采集线程池
        PartitionGather gather = new PartitionGather(register,firstServer,routine.checkVaildPartition(),5);
        DiskDaemon diskDaemon =  new DiskDaemon(gather,parts);
        System.out.println("-------------------------------------------------------");
    }
    @After
    public void closeAll(){
        if(client != null){
            client.close();
        }
    }
}
