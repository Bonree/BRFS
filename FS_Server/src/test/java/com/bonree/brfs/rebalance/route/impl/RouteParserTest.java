package com.bonree.brfs.rebalance.route.impl;

import com.bonree.brfs.common.rebalance.Constants;
import com.bonree.brfs.common.rebalance.route.VirtualRoute;
import com.bonree.brfs.common.rebalance.route.impl.SuperNormalRoute;
import com.bonree.brfs.common.utils.JsonUtils;
import com.bonree.brfs.common.zookeeper.curator.CuratorClient;
import com.bonree.brfs.rebalance.route.RouteLoader;
import com.fasterxml.jackson.core.type.TypeReference;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
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
 * @date 2020年03月24日 11:53:28
 * @author: <a href=mailto:zhucg@bonree.com>朱成岗</a>
 * @description:
 ******************************************************************************/

public class RouteParserTest {
    public static String ZK_ADDRESS = "192.168.101.87:2181";
    private static String resourcePath = RouteParserTest.class.getResource("/Routes/ZookeeperRoute").getPath();
    private static String V1_ARRAY = "V1Array.json";
    private static String V2_ARRAY = "V2Array.json";
    private static String MUL_ARRAY = "MULArray.json";
    private static String VIRTUAL_ARRAY = "VirtualArray.json";
    private static String V1_ROUTE_BAS_PATH = "/brfsDevTest/route/V1";
    private static String V2_ROUTE_BAS_PATH = "/brfsDevTest/route/V2";
    private static String MUL_VERSION_ROUTE_BAS_PATH = "/brfsDevTest/route/Mul";
    private static CuratorFramework client = null;
    private static int SR_ID = 0;

    private static CuratorClient zkClient = null;

    /**
     * 检查测试资源是否存在，若不存在，则测试失败
     */
    @Before
    public void checkLoad() throws Exception {
        //load();
    }

    public void load() {
        //1. 初始化zk客户端
        zkClient = CuratorClient.getClientInstance(ZK_ADDRESS);
        client = CuratorFrameworkFactory.newClient(ZK_ADDRESS, new RetryNTimes(10, 1000));
        try {
            client.start();
            client.blockUntilConnected();
            zkClient.blockUntilConnected(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Assert.fail("zookeeper is invaild ! address: " + ZK_ADDRESS);
        }
        // 2.创建虚拟路由规则
        createZkVirtualRouteData(zkClient, V1_ROUTE_BAS_PATH, VIRTUAL_ARRAY);
        createZkVirtualRouteData(zkClient, V2_ROUTE_BAS_PATH, VIRTUAL_ARRAY);
        createZkVirtualRouteData(zkClient, MUL_VERSION_ROUTE_BAS_PATH, VIRTUAL_ARRAY);
        // 3. 创建V1版本的路由规则
        createZkNormalData(zkClient, V1_ROUTE_BAS_PATH, V1_ARRAY);
        createZkNormalData(zkClient, V2_ROUTE_BAS_PATH, V2_ARRAY);
        createZkNormalData(zkClient, MUL_VERSION_ROUTE_BAS_PATH, MUL_ARRAY);
    }

    /**
     * 在zookeeper 创建路由规则
     *
     * @param client
     * @param zkNode
     * @param fileName
     */
    public void createZkNormalData(CuratorClient client, String zkNode, String fileName) {
        if (!client.checkExists(zkNode)) {
            client.createPersistent(zkNode, true);
        }
        List<SuperNormalRoute> routes = readNormalRoute(fileName);
        int srId = -1;
        String changeID = null;
        String nznode = null;
        for (SuperNormalRoute route : routes) {
            srId = route.getStorageIndex();
            changeID = route.getChangeID();
            nznode = zkNode + Constants.SEPARATOR + Constants.NORMAL_ROUTE + Constants.SEPARATOR + srId + Constants.SEPARATOR
                + changeID;
            if (!client.checkExists(nznode)) {
                client.createPersistent(nznode, true, JsonUtils.toJsonBytesQuietly(route));
            }
        }
    }

    /**
     * 在zookeeper 创建虚拟serverId的规则
     *
     * @param client
     * @param zkNode
     * @param fileName
     */
    public void createZkVirtualRouteData(CuratorClient client, String zkNode, String fileName) {
        if (!client.checkExists(zkNode)) {
            client.createPersistent(zkNode, true);
        }
        List<VirtualRoute> routes = readVirtualRoute(fileName);
        int srId = -1;
        String changeID = null;
        String nznode = null;
        for (VirtualRoute route : routes) {
            srId = route.getStorageIndex();
            changeID = route.getChangeID();
            nznode = zkNode + Constants.SEPARATOR + Constants.VIRTUAL_ROUTE + Constants.SEPARATOR + srId + Constants.SEPARATOR
                + changeID;
            if (!client.checkExists(nznode)) {
                client.createPersistent(nznode, true, JsonUtils.toJsonBytesQuietly(route));
            }
        }
    }

    /**
     * 读取正常路由规则
     *
     * @param fileName
     *
     * @return
     */
    public List<SuperNormalRoute> readNormalRoute(String fileName) {
        List<SuperNormalRoute> list = null;
        byte[] data = null;
        try {
            data = readBytesFromFile(fileName);
            list = JsonUtils.toObject(data, new TypeReference<List<SuperNormalRoute>>() {
            });
        } catch (JsonUtils.JsonException e) {
            Assert.fail("Deserlize byte[] fail " + data == null ? "" : new String(data));
        }
        return list;
    }

    /**
     * 读取虚拟路由规则
     *
     * @param fileName
     *
     * @return
     */
    public List<VirtualRoute> readVirtualRoute(String fileName) {
        List<VirtualRoute> list = null;
        byte[] data = null;
        try {
            data = readBytesFromFile(fileName);
            list = JsonUtils.toObject(data, new TypeReference<List<VirtualRoute>>() {
            });
        } catch (JsonUtils.JsonException e) {
            Assert.fail("Deserlize byte[] fail " + data == null ? "" : new String(data));
        }
        return list;
    }

    /**
     * 读取文件信息
     *
     * @param fileName
     *
     * @return
     */
    public byte[] readBytesFromFile(String fileName) {
        String filePath = resourcePath + File.separator + fileName;
        try {
            return FileUtils.readFileToByteArray(new File(filePath));
        } catch (IOException e) {
            Assert.fail("read file happen error !! file:" + filePath);
        }
        return null;
    }

    public String[] selectServerFromOld(String[] array) {
        String[] tmp = new String[array.length - 1];
        for (int i = 1; i < array.length; i++) {
            tmp[i - 1] = array[i];
        }
        return tmp;
    }

    /**
     * TestCase 1：
     * 测试包含虚拟serverId的旧的解析器与新的解析器是否兼容。
     * 正常的路由规则：
     * 20:[21,22,23]
     * 23:[21,22,24]
     * 21:[22,24]
     * 文件名 A_20_30
     * 预期：SecondIDParser的结果与RouteParser的结果保持一致
     *
     * @throws Exception
     */
    @Test
    public void compareOldToNewWithVirtural() throws Exception {
        RouteParser newParser = new RouteParser(SR_ID, new SimpleRouteZKLoader(client, V1_ROUTE_BAS_PATH));
        String fileName = "A_20_30";
        String[] newArray = newParser.searchVaildIds(fileName);
    }

    /**
     * TestCase 2：
     * 测试serverId的旧的解析器与新的解析器是否兼容。
     * 正常的路由规则：
     * 20:[21,22,23]
     * 23:[21,22,24]
     * 21:[22,24]
     * 文件名 A_20_24
     * 预期：SecondIDParser的结果与RouteParser的结果保持一致
     *
     * @throws Exception
     */
    @Test
    public void compareOldToNew() throws Exception {
        RouteParser newParser = new RouteParser(SR_ID, new SimpleRouteZKLoader(client, V1_ROUTE_BAS_PATH));
        String fileName = "A_20_24";
        String[] newArray = newParser.searchVaildIds(fileName);
    }

    /**
     * 测试V2版本路由规则，判断迁移的是否符合预期
     * 正常的路由规则：
     * 20:[21:1,22:4,23:5]
     * 23:[21:1,22:2,24:7]
     * 21:[22:4,24:6]
     * 文件名 A_20_24
     * 预期： [22,24]
     */
    @Test
    public void analysisV2Route() throws Exception {
        String fileName = "A_20_24";
        String[] expValue = {"22", "24"};
        RouteParser newParser = new RouteParser(SR_ID, new SimpleRouteZKLoader(client, V2_ROUTE_BAS_PATH));
        String[] newArray = newParser.searchVaildIds(fileName);
        Assert.assertArrayEquals(expValue, newArray);
    }

    @Test
    public void analysisV2Route02() throws Exception {
        String fileName = "084adb555f284f61b67158c83c065204_20_30";
        CuratorFramework client = CuratorFrameworkFactory
            .newClient("192.168.150.236:2181", new RetryNTimes(50, 1000));
        client.start();
        client.blockUntilConnected();
        RouteLoader loader = new SimpleRouteZKLoader(client, "/brfs/idea/routeSet");
        RouteParser parser = new RouteParser(0, loader);
        String[] array = parser.searchVaildIds(fileName);
        System.out.println(Arrays.asList(array));
    }

    @After
    public void close() {
        if (zkClient != null) {
            zkClient.close();
        }
    }
}
