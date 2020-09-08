package com.bonree.brfs.rebalance.route.impl;

import com.bonree.brfs.common.rebalance.Constants;
import com.bonree.brfs.common.rebalance.TaskVersion;
import com.bonree.brfs.common.rebalance.route.NormalRouteInterface;
import com.bonree.brfs.common.rebalance.route.VirtualRoute;
import com.bonree.brfs.common.rebalance.route.impl.SuperNormalRoute;
import com.bonree.brfs.common.utils.JsonUtils;
import com.fasterxml.jackson.core.type.TypeReference;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
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
 * @date 2020年03月31日 09:52:05
 * @author: <a href=mailto:zhucg@bonree.com>朱成岗</a>
 * @description:
 ******************************************************************************/

public class SimpleRouteZKLoaderTest {
    public static String ZK_ADDRESS = "192.168.101.87:2181";
    private static String resourcePath = SimpleRouteZKLoaderTest.class.getResource("/Routes/ZookeeperRoute").getPath();
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
        load();
    }

    public void load() {
        //1. 初始化zk客户端
        client = CuratorFrameworkFactory.newClient(ZK_ADDRESS, new RetryNTimes(10, 1000));
        try {
            client.start();
            client.blockUntilConnected();
            // 2.创建虚拟路由规则
            createZkVirtualRouteData(client, V1_ROUTE_BAS_PATH, VIRTUAL_ARRAY);
            createZkVirtualRouteData(client, V2_ROUTE_BAS_PATH, VIRTUAL_ARRAY);
            createZkVirtualRouteData(client, MUL_VERSION_ROUTE_BAS_PATH, VIRTUAL_ARRAY);
            // 3. 创建V1版本的路由规则
            createZkNormalData(client, V1_ROUTE_BAS_PATH, V1_ARRAY);
            createZkNormalData(client, V2_ROUTE_BAS_PATH, V2_ARRAY);
            createZkNormalData(client, MUL_VERSION_ROUTE_BAS_PATH, MUL_ARRAY);
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

    @Test
    public void loadNormalV1Test() throws Exception {
        SimpleRouteZKLoader loader = new SimpleRouteZKLoader(client, V1_ROUTE_BAS_PATH);
        Collection<NormalRouteInterface> list = loader.loadNormalRoutes(SR_ID);
        Assert.assertNotNull(list);
    }

    @Test
    public void loadVirtualV1Test() throws Exception {
        SimpleRouteZKLoader loader = new SimpleRouteZKLoader(client, V1_ROUTE_BAS_PATH);
        Collection<VirtualRoute> list = loader.loadVirtualRoutes(SR_ID);
        Assert.assertNotNull(list);
    }

    @Test
    public void loadNormalV2Test() throws Exception {
        SimpleRouteZKLoader loader = new SimpleRouteZKLoader(client, V2_ROUTE_BAS_PATH);
        Collection<NormalRouteInterface> list = loader.loadNormalRoutes(SR_ID);
        Assert.assertNotNull(list);
    }

    @Test
    public void loadVirtualV2Test() throws Exception {
        SimpleRouteZKLoader loader = new SimpleRouteZKLoader(client, V2_ROUTE_BAS_PATH);
        Collection<VirtualRoute> list = loader.loadVirtualRoutes(SR_ID);
        Assert.assertNotNull(list);
    }

    @Test
    public void loadNormalMULTest() throws Exception {
        SimpleRouteZKLoader loader = new SimpleRouteZKLoader(client, MUL_VERSION_ROUTE_BAS_PATH);
        Collection<NormalRouteInterface> list = loader.loadNormalRoutes(SR_ID);
        Assert.assertNotNull(list);
    }

    @Test
    public void loadVirtualMULTest() throws Exception {
        SimpleRouteZKLoader loader = new SimpleRouteZKLoader(client, MUL_VERSION_ROUTE_BAS_PATH);
        Collection<VirtualRoute> list = loader.loadVirtualRoutes(SR_ID);
        Assert.assertNotNull(list);
    }

    @Test
    public void filterInvalidVirtualTest() {
        List<VirtualRoute> list = new ArrayList<>();
        Set<String> onceVirtuals = new HashSet<>();
        VirtualRoute virtual1 = new VirtualRoute("1", 0, "29", "21", TaskVersion.V1);
        VirtualRoute virtual2 = new VirtualRoute("2", 0, "30", "21", TaskVersion.V1);
        VirtualRoute virtual3 = new VirtualRoute("3", 0, "30", "21", TaskVersion.V1);
        list.add(virtual1);
        list.add(virtual2);
        list.add(virtual3);
        onceVirtuals.add(virtual1.getVirtualID());
        onceVirtuals.add(virtual2.getVirtualID());
        onceVirtuals.add(virtual3.getVirtualID());
        List<VirtualRoute> results = new ArrayList<>();
        list.sort(new Comparator<VirtualRoute>() {
            @Override
            public int compare(VirtualRoute o1, VirtualRoute o2) {
                return o1.getChangeID().compareTo(o2.getChangeID());
            }
        });
        for (VirtualRoute route : list) {
            if (onceVirtuals.contains(route.getVirtualID())) {
                onceVirtuals.remove(route.getVirtualID());
                results.add(route);
            }
        }
        System.out.println(JsonUtils.toJsonStringQuietly(results));
    }

    @After
    public void close() {
        if (client != null) {
            client.close();
        }
    }
}
