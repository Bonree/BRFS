/*******************************************************************************
 * 版权信息： 北京博睿宏远数据科技股份有限公司
 * Copyright (c) 2007-2020 北京博睿宏远数据科技股份有限公司，Inc. All Rights Reserved.
 * @date 2020年03月13日 13:52:46
 * @author: <a href=mailto:zhucg@bonree.com>朱成岗</a>
 * @description: 解析路由规则测试类，采用Junit
 ******************************************************************************/

package com.bonree.brfs.common.rebalance.route.impl.v2;

import com.bonree.brfs.common.data.utils.JsonUtils;
import com.bonree.brfs.common.rebalance.route.NormalRouteInterface;
import com.bonree.brfs.common.rebalance.route.impl.SuperNormalRoute;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

public class NormalRouteV2Test {
    private String resourcePath = this.getClass().getResource("/NormalRouteV2Set").getPath();

    private final String v2RouteFile = "V2_NormalRoute.json";

    /**
     * 测试资源检查，若无测试资源，则测试失败
     */
    @Before
    public void checkTestFile() {
        String filePath = null;
        filePath = resourcePath + File.separator + v2RouteFile;
        if (!new File(filePath).exists()) {
            throw new IllegalArgumentException(v2RouteFile + "not exists !!! test break!!!");
        }
    }

    public byte[] readBytesFromFile(String fileName) throws IOException {
        String filePath = resourcePath + File.separator + fileName;
        return FileUtils.readFileToByteArray(new File(filePath));
    }

    /**
     * 序列化V2版本的json
     *
     * @throws IOException
     */
    @Test
    public void testSerializeV2() throws Exception {
        String changeId = "123456";
        String secondServer = "10";
        int storageId = 0;
        Map<String, Integer> serverMap = new HashMap<>();
        Map<String, String> ship = new HashMap<>();
        for (int index = 11; index < 14; index++) {
            serverMap.put(index + "", index % 5);
            ship.put(index + "", index + "");
        }
        NormalRouteV2 normalRouteV2 =
            new NormalRouteV2(changeId, storageId, secondServer, serverMap, ship);
        System.out.println(JsonUtils.toJsonString(normalRouteV2));
        String content =
            "{\"changeID\":\"123456\",\"storageIndex\":0,\"secondID\":\"10\","
                + "\"newSecondIDs\":{\"11\":1,\"12\":2,\"13\":3},\"secondFirstShip"
                + "\":{\"11\":\"11\",\"12\":\"12\",\"13\":\"13\"},\"version\":\"V2\"}";
        NormalRouteV2 vv2 = (NormalRouteV2) JsonUtils.toObjectQuietly(content, SuperNormalRoute.class);
        System.out.println(vv2);
    }

    /**
     * 反序列化V2版本的json
     *
     * @throws IOException
     */
    @Test
    public void testDeserializeV2() throws IOException {
        byte[] datas = readBytesFromFile(v2RouteFile);
        NormalRouteV2 routeV2 = JsonUtils.toObjectQuietly(datas, NormalRouteV2.class);
        Map<String, Object> map = JsonUtils.toObjectQuietly(datas, Map.class);
    }

    /**
     * 执行NormalRouteV2的内部方法 searchIndex
     *
     * @param fileName
     * @param services
     * @param weightValue
     *
     * @return
     */
    private int searchIndexRunCase(String fileName, List<String> services, int weightValue) {
        // 加载对象信息
        NormalRouteV2 routeV2 = null;
        routeV2 = getNormalRouteV2(fileName);

        try {
            Method searchMethod =
                routeV2.getClass().getDeclaredMethod("searchIndex", List.class, int.class);
            searchMethod.setAccessible(true);
            Object index = searchMethod.invoke(routeV2, services, weightValue);
            return (int) index;
        } catch (NoSuchMethodException e) {
            e.printStackTrace();
            Assert.fail("Run Method happen error ! " + e);
        } catch (IllegalAccessException e) {
            e.printStackTrace();
            Assert.fail("Run Method happen error ! " + e);
        } catch (InvocationTargetException e) {
            Assert.fail("Run Method happen error ! " + e);
        }
        return Integer.MIN_VALUE;
    }

    /**
     * 测试searchIndex 在正常取值范围的准确性
     * NormalRouteV2内容：{"changeID":"123456","storageIndex":0,"secondID":"10","newSecondIDs":{"11":1,"12":2,"13":3},"version":"V2"}
     * 可选服务列表：[11,12,13]
     * 权值为: 3
     * 预期数组序号: 1
     */
    @Test
    public void searchIndexNormalTest() {
        String fileName = v2RouteFile;
        List<String> services = Arrays.asList("11", "12", "13");
        int weight = 3;
        int expectIndex = 1;
        int index = searchIndexRunCase(fileName, services, weight);
        Assert.assertEquals(expectIndex, index);
    }

    /**
     * 测试searchIndex 在最大临界条件的准确性
     * NormalRouteV2内容：{"changeID":"123456","storageIndex":0,"secondID":"10","newSecondIDs":{"11":1,"12":2,"13":3},"version":"V2"}
     * 可选服务列表：[11,12,13]
     * 权值为: 5
     * 预期数组序号: 2
     */
    @Test
    public void searchIndexMaxBoundaryTest() {
        String fileName = v2RouteFile;
        List<String> services = Arrays.asList("11", "12", "13");
        int weight = 5;
        int expectIndex = 2;
        int index = searchIndexRunCase(fileName, services, weight);
        Assert.assertEquals(expectIndex, index);
    }

    /**
     * 测试searchIndex 在最小临界条件的准确性
     * NormalRouteV2内容：{"changeID":"123456","storageIndex":0,"secondID":"10","newSecondIDs":{"11":1,"12":2,"13":3},"version":"V2"}
     * 可选服务列表：[11,12,13]
     * 权值为: 0
     * 预期数组序号: 0
     */
    @Test
    public void searchIndexMinBoundaryTest() {
        String fileName = v2RouteFile;
        List<String> services = Arrays.asList("11", "12", "13");
        int weight = 0;
        int expectIndex = 0;
        int index = searchIndexRunCase(fileName, services, weight);
        Assert.assertEquals(expectIndex, index);
    }

    /**
     * 测试searchIndex 异常条件的结果
     * NormalRouteV2内容：{"changeID":"123456","storageIndex":0,"secondID":"10","newSecondIDs":{"11":1,"12":2,"13":3},"version":"V2"}
     * 可选服务列表：[11,12,13]
     * 权值为: -1
     * 预期: 0
     */
    @Test
    public void searchIndexMinExceptionTest() {
        String fileName = v2RouteFile;
        List<String> services = Arrays.asList("11", "12", "13");
        int weight = -1;
        int expectIndex = 0;
        int index = searchIndexRunCase(fileName, services, weight);
        Assert.assertEquals(expectIndex, index);
    }

    /**
     * 测试searchIndex 异常条件的结果
     * NormalRouteV2内容：{"changeID":"123456","storageIndex":0,"secondID":"10","newSecondIDs":{"11":1,"12":2,"13":3},"version":"V2"}
     * 可选服务列表：[11,12,13]
     * 权值为: 6
     * 预期: 2
     */
    @Test
    public void searchIndexMaxExceptionTest() {
        String fileName = v2RouteFile;
        List<String> services = Arrays.asList("11", "12", "13");
        int weight = 6;
        int expectIndex = 2;
        int index = searchIndexRunCase(fileName, services, weight);
        Assert.assertEquals(expectIndex, index);
    }

    /**
     * NormalRouteV2内部方法测试，方法calcWeight
     *
     * @param fileName
     * @param servers
     *
     * @return
     */
    private int calcWeightRunCase(String fileName, Collection<String> servers) {
        // 加载对象信息
        NormalRouteV2 routeV2 = null;
        routeV2 = getNormalRouteV2(fileName);

        try {
            Method searchMethod =
                routeV2.getClass().getDeclaredMethod("calcWeight", Collection.class);
            searchMethod.setAccessible(true);
            Object index = searchMethod.invoke(routeV2, servers);
            return (int) index;
        } catch (NoSuchMethodException e) {
            e.printStackTrace();
            Assert.fail("Run Method happen error ! " + e);
        } catch (IllegalAccessException e) {
            e.printStackTrace();
            Assert.fail("Run Method happen error ! " + e);
        } catch (InvocationTargetException e) {
            Assert.fail("Run Method happen error ! " + e);
        }
        return Integer.MIN_VALUE;
    }

    private NormalRouteV2 getNormalRouteV2(String fileName) {
        try {
            byte[] datas = readBytesFromFile(fileName);
            return JsonUtils.toObjectQuietly(datas, NormalRouteV2.class);
        } catch (IOException e) {
            Assert.fail(fileName + " deserialize NormalrouteV2 object fail !!!" + e);
        }
        return null;
    }

    /**
     * NormalRouteV2 内部方法测试 calcWeight测试 正常测试
     * NormalRouteV2内容：{"changeID":"123456","storageIndex":0,"secondID":"10","newSecondIDs":{"11":1,"12":2,"13":3},"version":"V2"}
     * 服务列表：[11,12,13]
     * 预期: 6
     */
    @Test
    public void calcWeightTest() {
        String fileName = v2RouteFile;
        List<String> servers = Arrays.asList("11", "12", "13");
        int expectWeight = 6;
        int weight = calcWeightRunCase(fileName, servers);
        Assert.assertEquals(expectWeight, weight);
    }

    /**
     * NormalRouteV2 内部方法测试 calcWeight测试 异常测试，输入为空
     * NormalRouteV2内容：{"changeID":"123456","storageIndex":0,"secondID":"10","newSecondIDs":{"11":1,"12":2,"13":3},"version":"V2"}
     * 服务列表：[]
     * 预期: -1
     */
    @Test
    public void calcWeightEmptyTest() {
        String fileName = v2RouteFile;
        List<String> servers = Arrays.asList();
        int expectWeight = -1;
        int weight = calcWeightRunCase(fileName, servers);
        Assert.assertEquals(expectWeight, weight);
    }

    /**
     * NormalRouteV2 内部方法测试 calcWeight测试 异常测试，输入无法匹配
     * NormalRouteV2内容：{"changeID":"123456","storageIndex":0,"secondID":"10","newSecondIDs":{"11":1,"12":2,"13":3},"version":"V2"}
     * 服务列表：["23","24"]
     * 预期: -1
     */
    @Test
    public void calcWeightNoMatchTest() {
        String fileName = v2RouteFile;
        List<String> servers = Arrays.asList("23", "24");
        int expectWeight = -1;
        int weight = calcWeightRunCase(fileName, servers);
        Assert.assertEquals(expectWeight, weight);
    }

    /**
     * NormalRouteV2内部方法测试，方法filterService
     *
     * @param fileName
     * @param servers
     *
     * @return
     */
    private List<String> filterServiceRunCase(String fileName, Collection<String> servers) {
        // 加载对象信息
        NormalRouteV2 routeV2 = null;
        routeV2 = getNormalRouteV2(fileName);

        try {
            Method searchMethod =
                routeV2.getClass().getDeclaredMethod("filterService", Collection.class);
            searchMethod.setAccessible(true);
            Object list = searchMethod.invoke(routeV2, servers);
            return (List<String>) list;
        } catch (NoSuchMethodException e) {
            e.printStackTrace();
            Assert.fail("Run Method happen error ! " + e);
        } catch (IllegalAccessException e) {
            e.printStackTrace();
            Assert.fail("Run Method happen error ! " + e);
        } catch (InvocationTargetException e) {
            e.printStackTrace();
            Assert.fail("Run Method happen error ! " + e);
        }
        return null;
    }

    /**
     * NormalRouteV2 内部方法 filterService 正常测试
     * NormalRouteV2内容：{"changeID":"123456","storageIndex":0,"secondID":"10","newSecondIDs":{"11":1,"12":2,"13":3},"version":"V2"}
     * 不可用数组：["11"]
     * 预期数组：["12","13"]
     */
    @Test
    public void filterServerNormalTest() {
        String fileName = v2RouteFile;
        Collection<String> servers = Arrays.asList("11");
        List<String> expectArray = Arrays.asList("12", "13");
        List<String> array = filterServiceRunCase(fileName, servers);
        Assert.assertArrayEquals(expectArray.toArray(), array.toArray());
    }

    /**
     * NormalRouteV2 内部方法 filterService 集合为空
     * NormalRouteV2内容：{"changeID":"123456","storageIndex":0,"secondID":"10","newSecondIDs":{"11":1,"12":2,"13":3},"version":"V2"}
     * 不可用数组：["11"]
     * 预期数组：["12","13"]
     */
    @Test
    public void filterServerEmptyTest() {
        String fileName = v2RouteFile;
        Collection<String> servers = Arrays.asList();
        List<String> expectArray = Arrays.asList("11", "12", "13");
        List<String> array = filterServiceRunCase(fileName, servers);
        Assert.assertArrayEquals(expectArray.toArray(), array.toArray());
    }

    /**
     * NormalRouteV2 内部方法 filterService 集合为空
     * NormalRouteV2内容：{"changeID":"123456","storageIndex":0,"secondID":"10","newSecondIDs":{"11":1,"12":2,"13":3},"version":"V2"}
     * 不可用数组：["11","12","13"]
     * 预期 ：IllegalArgumentException
     * 用例无法测试，原因反射机制针对调用方法产生的异常会发生InvocationTargetException
     */
    @Test(expected = IllegalArgumentException.class)
    @Ignore
    public void filterServerExceptionTest() {
        String fileName = v2RouteFile;
        Collection<String> servers = Arrays.asList("11", "12", "13");
        List<String> array = filterServiceRunCase(fileName, servers);
    }

    /**
     * 方法 locateNormalServer 正常测试
     * NormalRouteV2内容：{"changeID":"123456","storageIndex":0,"secondID":"10","newSecondIDs":{"11":1,"12":2,"13":3},"version":"V2"}
     * uuid:"A"
     * 服务列表：["11"]
     * 预期 ：12
     */
    @Test
    public void locateNormalServerNormalTest() {
        NormalRouteV2 route = getNormalRouteV2(v2RouteFile);
        String uuid = "A";
        List<String> servers = Arrays.asList("11");
        String expectServer = "12";
        int code = sumName(uuid);
        route.locateNormalServer(code, servers);
    }

    /**
     * 方法 locateNormalServer 无服务测试
     * NormalRouteV2内容：{"changeID":"123456","storageIndex":0,"secondID":"10","newSecondIDs":{"11":1,"12":2,"13":3},"version":"V2"}
     * uuid:"A"
     * 服务列表：["11"，"12","13"]
     * 预期 ：抛出异常IllegalArgumentException
     */
    @Test(expected = IllegalArgumentException.class)
    public void locateNormalServerExceptionTest() {
        NormalRouteV2 route = getNormalRouteV2(v2RouteFile);
        String uuid = "A";
        List<String> servers = Arrays.asList("11", "12", "13");
        int code = sumName(uuid);
        route.locateNormalServer(code, servers);

    }

    /**
     * 根据文件名生成code
     *
     * @param name
     *
     * @return
     */
    protected int sumName(String name) {
        int sum = 0;
        for (int i = 0; i < name.length(); i++) {
            sum = sum + name.charAt(i);
        }
        return sum;
    }

    @Test
    public void testAnalysis01() {
        String changeId = "1";
        int storageIndex = 0;
        String secondId = "22";
        Map<String, Integer> sizeMap = new HashMap<>();
        sizeMap.put("20", 1);
        sizeMap.put("21", 1);
        sizeMap.put("23", 1);
        Map<String, String> map = new HashMap<>();
        map.put("20", "1");
        map.put("21", "1");
        map.put("23", "3");
        NormalRouteV2 v2 = new NormalRouteV2(changeId, storageIndex, secondId, sizeMap, map);
        int code = 9;
        List<String> services = Arrays.asList("20", "22");
        String selector = v2.locateNormalServer(code, services);
        Assert.assertEquals("23", selector);

        code = 8;
        selector = v2.locateNormalServer(code, services);
        Assert.assertEquals("23", selector);

        services = Arrays.asList("23", "22");
        selector = v2.locateNormalServer(code, services);
        Assert.assertEquals("20", selector);
    }

    @Test
    public void testAnalysis02() {
        String changeId = "1";
        int storageIndex = 0;
        String secondId = "20";
        Map<String, Integer> sizeMap = new HashMap<>();
        sizeMap.put("22", 1);
        sizeMap.put("23", 1);
        sizeMap.put("24", 1);
        sizeMap.put("25", 1);
        Map<String, String> map = new HashMap<>();
        map.put("22", "1");
        map.put("23", "1");
        map.put("24", "2");
        map.put("25", "2");
        NormalRouteV2 v2 = new NormalRouteV2(changeId, storageIndex, secondId, sizeMap, map);
        int code = 9;
        List<String> services = Arrays.asList("20", "22");
        String selector = v2.locateNormalServer(code, services);
        Assert.assertEquals("25", selector);

        code = 8;
        selector = v2.locateNormalServer(code, services);
        Assert.assertEquals("24", selector);
    }
}
