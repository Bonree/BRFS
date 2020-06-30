package com.bonree.brfs.rebalance.route.impl;

import com.bonree.brfs.common.rebalance.Constants;
import com.bonree.brfs.common.rebalance.route.NormalRouteInterface;
import com.bonree.brfs.common.rebalance.route.VirtualRoute;
import com.bonree.brfs.common.rebalance.route.impl.SuperNormalRoute;
import com.bonree.brfs.common.utils.JsonUtils;
import com.bonree.brfs.rebalance.route.RouteLoader;
import com.fasterxml.jackson.core.type.TypeReference;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.FileUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryNTimes;
import org.apache.zookeeper.CreateMode;
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

    /**
     * 检查测试资源是否存在，若不存在，则测试失败
     */
    @Before
    public void checkLoad() throws Exception {
        //load();
    }

    public void load() {
        //1. 初始化zk客户端
        client = CuratorFrameworkFactory.newClient(ZK_ADDRESS, new RetryNTimes(10, 1000));
        try {
            client.start();
            client.blockUntilConnected();
            createZkNormalData(client, V1_ROUTE_BAS_PATH, V1_ARRAY);
            createZkNormalData(client, V2_ROUTE_BAS_PATH, V2_ARRAY);
            createZkVirtualRouteData(client, V1_ROUTE_BAS_PATH, VIRTUAL_ARRAY);
            createZkVirtualRouteData(client, V2_ROUTE_BAS_PATH, VIRTUAL_ARRAY);
        } catch (Exception e) {
            Assert.fail("zookeeper is invaild ! address: " + ZK_ADDRESS);
        }
    }

    /**
     * 在zookeeper 创建路由规则
     *
     * @param client
     * @param zkNode
     * @param fileName
     */
    public void createZkNormalData(CuratorFramework client, String zkNode, String fileName) throws Exception {
        if (client.checkExists().forPath(zkNode) == null) {
            client.create()
                  .creatingParentsIfNeeded()
                  .withMode(CreateMode.PERSISTENT)
                  .forPath(zkNode);
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
            if (client.checkExists().forPath(nznode) == null) {
                client.create()
                      .creatingParentsIfNeeded()
                      .withMode(CreateMode.PERSISTENT)
                      .forPath(nznode, JsonUtils.toJsonBytesQuietly(route));
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
    public void createZkVirtualRouteData(CuratorFramework client, String zkNode, String fileName) throws Exception {
        if (client.checkExists().forPath(zkNode) == null) {
            client.create()
                  .creatingParentsIfNeeded()
                  .withMode(CreateMode.PERSISTENT)
                  .forPath(zkNode);
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
            if (client.checkExists().forPath(nznode) == null) {
                client.create()
                      .creatingParentsIfNeeded()
                      .withMode(CreateMode.PERSISTENT)
                      .forPath(nznode, JsonUtils.toJsonBytesQuietly(route));
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
        String fileName = "e8b5e89297e04d6eae56e89c8fcf0297_22_20";
        CuratorFramework client = CuratorFrameworkFactory
            .newClient("192.168.150.105:2181", new RetryNTimes(50, 1000));
        client.start();
        client.blockUntilConnected();
        RouteLoader loader = new SimpleRouteZKLoader(client, "/brfs/brfs_lqtest/routeSet");
        RouteParser parser = new RouteParser(2, loader);
        String[] array = parser.searchVaildIds(fileName);
        System.out.println(Arrays.asList(array));
    }

    @Test
    public void analysisV2Route09() throws Exception {
        String content = "[\n"
            + "{\"changeID\":\"1593396649f41439ea-d566-422e-bb39-ad339f545d1e\","
            + "\"storageIndex\":0,\"secondID\":\"20\","
            + "\"newSecondIDs\":{\"22\":18123228,\"23\":516821060,\"21\":8120992},"
            + "\"secondFirstShip\":{\"22\":\"10\",\"23\":\"12\",\"20\":\"11\",\"21\":\"10\"},"
            + "\"version\":\"V2\"},\n"
            + "{\"changeID\":\"15933969071f7b5c9b-a58d-4fa2-8f84-a293ccbb1560\","
            + "\"storageIndex\":0,\"secondID\":\"22\","
            + "\"newSecondIDs\":{\"23\":516755864,\"24\":521775692},"
            + "\"secondFirstShip\":{\"22\":\"10\",\"23\":\"12\",\"24\":\"11\",\"21\":\"10\"},"
            + "\"version\":\"V2\"},\n"
            + "{\"changeID\":\"1593396912478030a4-e593-4b6f-960c-7f4f444df999\","
            + "\"storageIndex\":0,\"secondID\":\"21\","
            + "\"newSecondIDs\":{\"23\":516755864,\"24\":521775696},"
            + "\"secondFirstShip\":{\"22\":\"10\",\"23\":\"12\",\"24\":\"11\",\"21\":\"10\"},"
            + "\"version\":\"V2\"}\n"
            + "]";
        // content = "[\n"
        //     + "{\"changeID\":\"1593396649f41439ea-d566-422e-bb39-ad339f545d1e\","
        //     + "\"storageIndex\":0,\"secondID\":\"20\","
        //     + "\"newSecondIDs\":{\"22\":18123228,\"23\":516821060,\"21\":8120992},"
        //     + "\"secondFirstShip\":{\"22\":\"10\",\"23\":\"12\",\"20\":\"11\",\"21\":\"10\"},"
        //     + "\"version\":\"V2\"}]";

        String fileblockname1 = "64ba3c5d33cf4fe2a11cddefaba55d8b_22_20";
        String fileblockname2 = "6901c385f0d047579684984c06ca51bd_20_22";

        NormalRouteInterface[] routes = JsonUtils.toObject(content, SuperNormalRoute[].class);

        RouteParser parser = new RouteParser(0, null, false);

        Arrays.asList(routes).forEach(
            route -> {
                parser.putNormalRoute(route);
            }
        );
        System.out.println(fileblockname1 + " search " + analysisroute(parser, fileblockname1));
        System.out.println(fileblockname2 + " search " + analysisroute(parser, fileblockname2));
    }

    private List<String> analysisroute(RouteParser parser, String fileblockname1) {
        String[] fields = parser.searchVaildIds(fileblockname1);
        return Arrays.asList(fields);
    }

    @Test
    public void analysisV2Route10() throws Exception {
        String content = "[{\"changeID\":\"15934266725f4cd6c5-682c-4261-a4bd-1498f91b3df5\","
            + "\"storageIndex\":0,\"secondID\":\"21\","
            + "\"newSecondIDs\":{\"22\":17979332,\"20\":8120992},"
            + "\"secondFirstShip\":{\"22\":\"10\",\"20\":\"10\",\"21\":\"11\"},"
            + "\"version\":\"V2\"}]";

        String fileblockname1 = "d0003118fe124d948700149a2a1ee295_21_22";

        NormalRouteInterface[] routes = JsonUtils.toObject(content, SuperNormalRoute[].class);

        RouteParser parser = new RouteParser(0, null, false);

        Arrays.asList(routes).forEach(
            route -> {
                parser.putNormalRoute(route);
            }
        );
        System.out.println(fileblockname1 + " search " + analysisroute(parser, fileblockname1));
    }

    @Test
    public void testSameOne() throws Exception {
        Map<String, String> map = new HashMap<>();
        map.put("20", "10");
        map.put("21", "10");
        map.put("22", "11");
        List<String> seconds = Arrays.asList("20", "21", "211");
        System.out.println(isSameFirst(map, seconds));
    }

    private boolean isSameFirst(Map<String, String> secondFirstMap, List<String> seconds) {
        Map<String, Integer> countMap = new HashMap<>();
        seconds.stream().forEach(second -> {
            String first = secondFirstMap.get(second);
            if (countMap.get(first) == null) {
                countMap.put(first, 1);
            } else {
                countMap.put(first, countMap.get(first) + 1);
            }
        });
        return countMap.size() <= 1;
    }

}
