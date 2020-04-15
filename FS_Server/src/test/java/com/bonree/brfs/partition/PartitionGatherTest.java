package com.bonree.brfs.partition;

import java.io.File;
import java.util.Collection;
import java.util.List;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryNTimes;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.bonree.brfs.common.service.Service;
import com.bonree.brfs.identification.impl.DiskNodeIDImpl;
import com.bonree.brfs.partition.model.LocalPartitionInfo;
import com.bonree.brfs.rebalance.route.impl.RouteParserTest;
import com.bonree.brfs.resourceschedule.utils.LibUtils;
import com.google.common.collect.ImmutableList;

/*******************************************************************************
 * 版权信息： 北京博睿宏远数据科技股份有限公司
 * Copyright (c) 2007-2020 北京博睿宏远数据科技股份有限公司，Inc. All Rights Reserved.
 * @date 2020年03月25日 19:09:19
 * @author: <a href=mailto:zhucg@bonree.com>朱成岗</a>
 * @description: 测试采集进程
 ******************************************************************************/

public class PartitionGatherTest {
    private static String FILE_DIR = PartitionGatherTest.class.getResource("/DiskNodeId").getPath();
    private static String EMPTY_DIR = FILE_DIR+ File.separator+"empty";
    private static String ONE_DIR = FILE_DIR+ File.separator+"one";
    private static String ID_BAS_PATH="/brfsDevTest/discovery";
    private static String BASE_ID_PATH = "/brfsDevTest/identification/server_id_sequences";
    private static String ZKADDRES = RouteParserTest.ZK_ADDRESS;
    private CuratorFramework framework = null;
    private DiskNodeIDImpl idImpl = null;
    private Service firstServer;
    private PartitionInfoRegister register = null;
    private PartitionCheckingRoutine routine;
    private List<String> dataDir = ImmutableList.of("/data/brfs/data");
    private String partitionGroup = "diskNodeIDS";
    @Before
    public void checkZK(){
        framework = CuratorFrameworkFactory.newClient(ZKADDRES,new RetryNTimes(5,300));
        framework.start();
        try {
            framework.blockUntilConnected();
        } catch (InterruptedException e) {
            Assert.fail("zookeeper client is invaild !! address: "+ZKADDRES);
        }
        idImpl = new DiskNodeIDImpl(framework,BASE_ID_PATH);
        firstServer = new Service("10","dataGroup","127.0.0.1",13000,System.currentTimeMillis());
        String libPath = "/data/brfs/lib";
        File file = new File(libPath);
        if(!file.exists()){
            Assert.fail("sigar lib add happen error path : "+libPath);
        }
        try {
            LibUtils.loadLibraryPath(libPath);
        } catch (Exception e) {
            e.printStackTrace();
        }
        routine =  new PartitionCheckingRoutine(idImpl,dataDir,ONE_DIR,partitionGroup);
        this.register = new PartitionInfoRegister(framework,ID_BAS_PATH);
    }

    /**
     * 测试构建方法
     */
    @Test
    public void constructorTest(){
        Collection<LocalPartitionInfo> parts =  routine.checkVaildPartition();
        System.out.println(parts);
        PartitionGather gather = new PartitionGather(this.register,firstServer,parts,10);
    }
    @Test
    public void startTest()throws Exception{
        Collection<LocalPartitionInfo> parts =  routine.checkVaildPartition();
        System.out.println(parts);
        PartitionGather gather = new PartitionGather(this.register,firstServer,parts,5);
        gather.start();
        Thread.sleep(Long.MAX_VALUE);
    }
    @Test
    public void gatherThreadTest(){
        Collection<LocalPartitionInfo> parts =  routine.checkVaildPartition();
        System.out.println(parts);
        PartitionGather.GatherThread gatherThread = new PartitionGather.GatherThread(this.register,parts,firstServer);
        gatherThread.run();
    }
    @After
    public void closeAll(){
        if(framework != null){
            framework.close();
        }
    }
}
