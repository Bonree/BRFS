package com.bonree.brfs.partition;

import com.bonree.brfs.common.resource.ResourceCollectionInterface;
import com.bonree.brfs.common.service.Service;
import com.bonree.brfs.identification.impl.DiskNodeIDImpl;
import com.bonree.brfs.partition.model.LocalPartitionInfo;
import com.bonree.brfs.rebalance.route.impl.RouteParserTest;
import com.bonree.brfs.resource.impl.SigarGather;
import com.google.common.collect.ImmutableList;
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.apache.commons.io.FileUtils;
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
 * @date 2020年03月25日 15:11:21
 * @author: <a href=mailto:zhucg@bonree.com>朱成岗</a>
 * @description: 磁盘分区检查
 ******************************************************************************/

public class PartitionCheckingRoutineTest {
    private static String FILE_DIR = PartitionCheckingRoutineTest.class.getResource("/DiskNodeId").getPath();
    private static String EMPTY_DIR = FILE_DIR + File.separator + "empty";
    private static String ONE_DIR = FILE_DIR + File.separator + "one";
    private static String BASE_ID_PATH = "/brfsDevTest/identification/server_id_sequences";
    private static String SECOND_ID_PATH = "/brfsDevTest/identification/server_ids";
    private static String ZKADDRES = RouteParserTest.ZK_ADDRESS;
    private CuratorFramework framework = null;
    private DiskNodeIDImpl idImpl = null;
    private Service firstServer;
    private ResourceCollectionInterface rGather = null;

    @Before
    public void checkZK() throws Exception {
        framework = CuratorFrameworkFactory.newClient(ZKADDRES, new RetryNTimes(5, 300));
        framework.start();
        try {
            framework.blockUntilConnected();
        } catch (InterruptedException e) {
            Assert.fail("zookeeper client is invaild !! address: " + ZKADDRES);
        }
        idImpl = new DiskNodeIDImpl(framework, BASE_ID_PATH, SECOND_ID_PATH);
        firstServer = new Service("10", "dataGroup", "127.0.0.1", 13000, System.currentTimeMillis());
        rGather = new SigarGather();
        rGather.start();
    }

    /**
     * 构造方法测试
     */
    @Test
    public void constructorTest() {
        List<String> dataDir = ImmutableList.of("C:/");
        String partitionGroup = "diskPartitionGroup";
        PartitionCheckingRoutine routine = new PartitionCheckingRoutine(idImpl, rGather, dataDir, FILE_DIR, partitionGroup);
    }

    /**
     * 读取空id文件目录
     */
    @Test
    public void readIdsEmptyTest() {
        List<String> dataDir = ImmutableList.of("/");
        String partitionGroup = "diskPartitionGroup";
        String idsPath = EMPTY_DIR;
        File idsFile = new File(idsPath);
        if (!idsFile.exists()) {
            try {
                FileUtils.forceMkdir(idsFile);
            } catch (IOException e) {
                e.printStackTrace();
                Assert.fail(idsPath + " can't create");
            }
        }
        PartitionCheckingRoutine routine = new PartitionCheckingRoutine(idImpl, rGather, dataDir, idsPath, partitionGroup);
        Map<String, LocalPartitionInfo> files = routine.readIds(idsPath);
        System.out.println(files);
//        Assert.assertNull(files);
    }

    /**
     * 测试获取文件系统
     */
    @Test
    public void collectVaildFileSystemNormal() {
        List<String> dataDir = ImmutableList.of("D:/");
        String partitionGroup = "diskPartitionGroup";
        String idsPath = EMPTY_DIR;
        File idsFile = new File(idsPath);
        if (!idsFile.exists()) {
            try {
                FileUtils.forceMkdir(idsFile);
            } catch (IOException e) {
                e.printStackTrace();
                Assert.fail(idsPath + " can't create");
            }
        }
        PartitionCheckingRoutine routine = new PartitionCheckingRoutine(idImpl, rGather, dataDir, idsPath, partitionGroup);
        String[] dirs = {"D:/"};
        Map<String, LocalPartitionInfo> map = routine.collectVaildFileSystem(dirs);
        Assert.assertEquals(dirs.length, map.size());

    }

    /**
     * 测试，当同一个分区上设置了多个目录时，将抛出异常
     */
    @Test(expected = RuntimeException.class)
    public void collectVaildFileSystemSameDir() {
        List<String> dataDir = ImmutableList.of("D:/zhucg/tmp");
        String partitionGroup = "diskPartitionGroup";
        String idsPath = EMPTY_DIR;
        File idsFile = new File(idsPath);
        if (!idsFile.exists()) {
            try {
                FileUtils.forceMkdir(idsFile);
            } catch (IOException e) {
                e.printStackTrace();
                Assert.fail(idsPath + " can't create");
            }
        }
        PartitionCheckingRoutine routine = new PartitionCheckingRoutine(idImpl, rGather, dataDir, idsPath, partitionGroup);
        String[] dirs = {"D:/zhucg/tmp", "D:/zhucg"};
        Map<String, LocalPartitionInfo> map = routine.collectVaildFileSystem(dirs);
        System.out.println(map.keySet());
    }

    @Test
    public void overallProcessWithNoInnerTest() {
        List<String> dataDir = ImmutableList.of("D:/zhucg/tmp");
        String partitionGroup = "diskPartitionGroup";
        String idsPath = EMPTY_DIR;
        File idsFile = new File(idsPath);
        if (!idsFile.exists()) {
            try {
                FileUtils.forceMkdir(idsFile);
            } catch (IOException e) {
                e.printStackTrace();
                Assert.fail(idsPath + " can't create");
            }
        }
        PartitionCheckingRoutine routine = new PartitionCheckingRoutine(idImpl, rGather, dataDir, idsPath, partitionGroup);
        routine.checkVaildPartition();

    }

    @Test
    public void overallProcessWithSingleFileTest() {
        List<String> dataDir = ImmutableList.of("D:/zhucg/tmp");
        String partitionGroup = "diskPartitionGroup";
        String idsPath = ONE_DIR;
        File idsFile = new File(idsPath);
        if (!idsFile.exists()) {
            try {
                FileUtils.forceMkdir(idsFile);
            } catch (IOException e) {
                e.printStackTrace();
                Assert.fail(idsPath + " can't create");
            }
        }
        PartitionCheckingRoutine routine = new PartitionCheckingRoutine(idImpl, rGather, dataDir, idsPath, partitionGroup);
        routine.checkVaildPartition();

    }

    @Test
    public void overallProcessWithRemoveTest() {
        List<String> dataDir = ImmutableList.of("D:/zhucg/tmp");
        String partitionGroup = "diskPartitionGroup";
        String idsPath = ONE_DIR;
        File idsFile = new File(idsPath);
        if (!idsFile.exists()) {
            try {
                FileUtils.forceMkdir(idsFile);
            } catch (IOException e) {
                e.printStackTrace();
                Assert.fail(idsPath + " can't create");
            }
        }
        PartitionCheckingRoutine routine = new PartitionCheckingRoutine(idImpl, rGather, dataDir, idsPath, partitionGroup);
        System.out.println(routine.checkVaildPartition());

    }

    @After
    public void closeAll() {
        if (framework != null) {
            framework.close();
        }
    }

}
