/*******************************************************************************
 * 版权信息： 北京博睿宏远数据科技股份有限公司
 * Copyright (c) 2007-2020 北京博睿宏远数据科技股份有限公司，Inc. All Rights Reserved.
 * @date 2020年03月13日 13:52:46
 * @author: <a href=mailto:zhucg@bonree.com>朱成岗</a>
 * @description: 解析路由规则测试类，采用Junit
 ******************************************************************************/

package com.bonree.brfs.common.rebalance.route.v2;

import com.bonree.brfs.common.data.utils.JsonUtils;
import org.apache.commons.io.FileUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.*;

public class NormalRouteV2Test {
    private String resourcePath = this.getClass().getResource("/NormalRouteV2Set").getPath();

    private final String V2RouteFile = "V2_NormalRoute.json";

    /**
     * 测试资源检查，若无测试资源，则测试失败
     */
    @Before
    public void checkTestFile(){
        String filePath = null;
        filePath = resourcePath+ File.separator+V2RouteFile;
        if(!new File(filePath).exists()){
            throw new IllegalArgumentException(V2RouteFile+"not exists !!! test break!!!");
        }
    }
    public byte[] readBytesFromFile(String fileName)throws IOException {
        String filePath = resourcePath+ File.separator+fileName;
        return FileUtils.readFileToByteArray(new File(filePath));
    }
    /**
     * 序列化V2版本的json
     * @throws IOException
     */
    @Test
    public void testSerializeV2()throws Exception{
        String filePath =resourcePath+File.separator+"V2.json";
        System.out.println(filePath);
        String changeId = "123456";
        String  secondServer = "10";
        int storageId = 0;
        Map<String,Integer> serverMap = new HashMap<>();
        for(int index=11;index <14;index++){
            serverMap.put(index+"",index%5);
        }
        NormalRouteV2 normalRouteV2 = new NormalRouteV2(changeId,storageId,secondServer,serverMap);
        byte[] data = JsonUtils.toJsonBytesQuietly(normalRouteV2);
        FileUtils.writeByteArrayToFile(new File(filePath),data);
    }


    /**
     * 反序列化V2版本的json
     * @throws IOException
     */
    @Test
    public void testDeserializeV2()throws IOException{
        byte[] datas = readBytesFromFile(V2RouteFile);
        NormalRouteV2 routeV2 = JsonUtils.toObjectQuietly(datas,NormalRouteV2.class);
        Map<String,Object> map = JsonUtils.toObjectQuietly(datas,Map.class);
    }
    /**
     * 执行NormalRouteV2的内部方法 searchIndex
     * @param fileName
     * @param services
     * @param weightValue
     * @return
     */
    private int searchIndexRunCase(String fileName,List<String> services,int weightValue){
        // 加载对象信息
        NormalRouteV2 routeV2 =null;
        routeV2 = getNormalRouteV2(fileName);

        try {
            Method searchMethod = routeV2.getClass().getDeclaredMethod("searchIndex", List.class,int.class);
            searchMethod.setAccessible(true);
            Object index = searchMethod.invoke(routeV2,services,weightValue);
            return (int) index;
        } catch (NoSuchMethodException e) {
            e.printStackTrace();
            Assert.fail("Run Method happen error ! " +e);
        } catch (IllegalAccessException e) {
            e.printStackTrace();
            Assert.fail("Run Method happen error ! " +e);
        } catch (InvocationTargetException e) {
            Assert.fail("Run Method happen error ! " +e);
        }
        return Integer.MIN_VALUE;
    }

    /**
     * 测试searchIndex 在正常取值范围的准确性
     *  NormalRouteV2内容：{"changeID":"123456","storageIndex":0,"secondID":"10","newSecondIDs":{"11":1,"12":2,"13":3},"version":"V2"}
     * 可选服务列表：[11,12,13]
     * 权值为: 3
     * 预期数组序号: 1
     */
    @Test
    public void searchIndexNormalTest(){
        String fileName = V2RouteFile;
        List<String> services = Arrays.asList("11","12","13");
        int weight = 3;
        int expectIndex = 1;
        int index = searchIndexRunCase(fileName,services,weight);
        Assert.assertEquals(expectIndex,index);
    }

    /**
     * 测试searchIndex 在最大临界条件的准确性
     *  NormalRouteV2内容：{"changeID":"123456","storageIndex":0,"secondID":"10","newSecondIDs":{"11":1,"12":2,"13":3},"version":"V2"}
     * 可选服务列表：[11,12,13]
     * 权值为: 5
     * 预期数组序号: 2
     */
    @Test
    public void searchIndexMaxBoundaryTest(){
        String fileName = V2RouteFile;
        List<String> services = Arrays.asList("11","12","13");
        int weight = 5;
        int expectIndex = 2;
        int index = searchIndexRunCase(fileName,services,weight);
        Assert.assertEquals(expectIndex,index);
    }
    /**
     * 测试searchIndex 在最小临界条件的准确性
     *  NormalRouteV2内容：{"changeID":"123456","storageIndex":0,"secondID":"10","newSecondIDs":{"11":1,"12":2,"13":3},"version":"V2"}
     * 可选服务列表：[11,12,13]
     * 权值为: 0
     * 预期数组序号: 0
     */
    @Test
    public void searchIndexMinBoundaryTest(){
        String fileName = V2RouteFile;
        List<String> services = Arrays.asList("11","12","13");
        int weight = 0;
        int expectIndex = 0;
        int index = searchIndexRunCase(fileName,services,weight);
        Assert.assertEquals(expectIndex,index);
    }
    /**
     * 测试searchIndex 异常条件的结果
     *  NormalRouteV2内容：{"changeID":"123456","storageIndex":0,"secondID":"10","newSecondIDs":{"11":1,"12":2,"13":3},"version":"V2"}
     * 可选服务列表：[11,12,13]
     * 权值为: -1
     * 预期: 0
     */
    @Test
    public void searchIndexMinExceptionTest(){
        String fileName = V2RouteFile;
        List<String> services = Arrays.asList("11","12","13");
        int weight = -1;
        int expectIndex=0;
        int index = searchIndexRunCase(fileName,services,weight);
        Assert.assertEquals(expectIndex,index);
    }
    /**
     * 测试searchIndex 异常条件的结果
     * NormalRouteV2内容：{"changeID":"123456","storageIndex":0,"secondID":"10","newSecondIDs":{"11":1,"12":2,"13":3},"version":"V2"}
     * 可选服务列表：[11,12,13]
     * 权值为: 6
     * 预期: 2
     */
    @Test
    public void searchIndexMaxExceptionTest(){
        String fileName = V2RouteFile;
        List<String> services = Arrays.asList("11","12","13");
        int weight = 6;
        int expectIndex=2;
        int index = searchIndexRunCase(fileName,services,weight);
        Assert.assertEquals(expectIndex,index);
    }

    /**
     * NormalRouteV2内部方法测试，方法hashFileName
     * @param fileName
     * @param uuid
     * @param weight
     * @return
     */
    private int hashFileNameRunCase(String fileName,String uuid,int weight){
        // 加载对象信息
        NormalRouteV2 routeV2 =null;
        routeV2 = getNormalRouteV2(fileName);

        try {
            Method searchMethod = routeV2.getClass().getSuperclass().getDeclaredMethod("hashFileName", String.class,int.class);
            searchMethod.setAccessible(true);
            Object index = searchMethod.invoke(routeV2,uuid,weight);
            return (int) index;
        } catch (NoSuchMethodException e) {
            e.printStackTrace();
            Assert.fail("Run Method happen error ! " +e);
        } catch (IllegalAccessException e) {
            e.printStackTrace();
            Assert.fail("Run Method happen error ! " +e);
        } catch (InvocationTargetException e) {
            Assert.fail("Run Method happen error ! " +e);
        }
        return Integer.MIN_VALUE;
    }

    /**
     * hashFilename功能测试
     * uuid： "A"
     * 权值： 3
     * 预期： 2
     */
   @Test
    public void HashFileNameNormalTest(){
        String fileName = V2RouteFile;
        String uuid="A";
        int weightCode=3;
        int expectIndex =2;
        int hashCode = hashFileNameRunCase(fileName,uuid,weightCode);
        Assert.assertEquals(expectIndex,hashCode);
   }

    /**
     * NormalRouteV2内部方法测试，方法calcWeight
     * @param fileName
     * @param servers
     * @return
     */
    private int calcWeightRunCase(String fileName, Collection<String> servers){
        // 加载对象信息
        NormalRouteV2 routeV2 =null;
        routeV2 = getNormalRouteV2(fileName);

        try {
            Method searchMethod = routeV2.getClass().getDeclaredMethod("calcWeight", Collection.class);
            searchMethod.setAccessible(true);
            Object index = searchMethod.invoke(routeV2,servers);
            return (int) index;
        } catch (NoSuchMethodException e) {
            e.printStackTrace();
            Assert.fail("Run Method happen error ! " +e);
        } catch (IllegalAccessException e) {
            e.printStackTrace();
            Assert.fail("Run Method happen error ! " +e);
        } catch (InvocationTargetException e) {
            Assert.fail("Run Method happen error ! " +e);
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
    public void calcWeightTest(){
        String fileName = V2RouteFile;
        List<String> servers = Arrays.asList("11","12","13");
        int expectWeight = 6;
        int weight = calcWeightRunCase(fileName,servers);
        Assert.assertEquals(expectWeight,weight);
    }
    /**
     * NormalRouteV2 内部方法测试 calcWeight测试 异常测试，输入为空
     * NormalRouteV2内容：{"changeID":"123456","storageIndex":0,"secondID":"10","newSecondIDs":{"11":1,"12":2,"13":3},"version":"V2"}
     * 服务列表：[]
     * 预期: -1
     */
    @Test
    public void calcWeightEmptyTest(){
        String fileName = V2RouteFile;
        List<String> servers = Arrays.asList();
        int expectWeight = -1;
        int weight = calcWeightRunCase(fileName,servers);
        Assert.assertEquals(expectWeight,weight);
    }
    /**
     * NormalRouteV2 内部方法测试 calcWeight测试 异常测试，输入无法匹配
     * NormalRouteV2内容：{"changeID":"123456","storageIndex":0,"secondID":"10","newSecondIDs":{"11":1,"12":2,"13":3},"version":"V2"}
     * 服务列表：["23","24"]
     * 预期: -1
     */
    @Test
    public void calcWeightNoMatchTest(){
        String fileName = V2RouteFile;
        List<String> servers = Arrays.asList("23","24");
        int expectWeight = -1;
        int weight = calcWeightRunCase(fileName,servers);
        Assert.assertEquals(expectWeight,weight);
    }

    /**
     * NormalRouteV2内部方法测试，方法filterService
     * @param fileName
     * @param servers
     * @return
     */
    private List<String> filterServiceRunCase(String fileName, Collection<String> servers){
        // 加载对象信息
        NormalRouteV2 routeV2 =null;
        routeV2 = getNormalRouteV2(fileName);

        try {
            Method searchMethod = routeV2.getClass().getDeclaredMethod("filterService", Collection.class);
            searchMethod.setAccessible(true);
            Object list = searchMethod.invoke(routeV2,servers);
            return (List<String>) list;
        } catch (NoSuchMethodException e) {
            e.printStackTrace();
            Assert.fail("Run Method happen error ! " +e);
        } catch (IllegalAccessException e) {
            e.printStackTrace();
            Assert.fail("Run Method happen error ! " +e);
        } catch (InvocationTargetException e) {
            e.printStackTrace();
            Assert.fail("Run Method happen error ! " +e);
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
    public void filterServerNormalTest(){
        String fileName = V2RouteFile;
        Collection<String> servers = Arrays.asList("11");
        List<String> expectArray = Arrays.asList("12","13");
        List<String> array = filterServiceRunCase(fileName,servers);
        Assert.assertArrayEquals(expectArray.toArray(),array.toArray());
    }
    /**
     * NormalRouteV2 内部方法 filterService 集合为空
     * NormalRouteV2内容：{"changeID":"123456","storageIndex":0,"secondID":"10","newSecondIDs":{"11":1,"12":2,"13":3},"version":"V2"}
     * 不可用数组：["11"]
     * 预期数组：["12","13"]
     */
    @Test
    public void filterServerEmptyTest(){
        String fileName = V2RouteFile;
        Collection<String> servers = Arrays.asList();
        List<String> expectArray = Arrays.asList("11","12","13");
        List<String> array = filterServiceRunCase(fileName,servers);
        Assert.assertArrayEquals(expectArray.toArray(),array.toArray());
    }
    /**
     * NormalRouteV2 内部方法 filterService 集合为空
     * NormalRouteV2内容：{"changeID":"123456","storageIndex":0,"secondID":"10","newSecondIDs":{"11":1,"12":2,"13":3},"version":"V2"}
     * 不可用数组：["11","12","13"]
     * 预期 ：IllegalArgumentException
     * 用例无法测试，原因反射机制针对调用方法产生的异常会发生InvocationTargetException
     */
    @Test(expected=IllegalArgumentException.class)
    @Ignore
    public void filterServerExceptionTest(){
        String fileName = V2RouteFile;
        Collection<String> servers = Arrays.asList("11","12","13");
        List<String> array = filterServiceRunCase(fileName,servers);
    }
    /**
     * 方法 locateNormalServer 正常测试
     * NormalRouteV2内容：{"changeID":"123456","storageIndex":0,"secondID":"10","newSecondIDs":{"11":1,"12":2,"13":3},"version":"V2"}
     * uuid:"A"
     * 服务列表：["11"]
     * 预期 ：12
     */
    @Test
    public void locateNormalServerNormalTest(){
        NormalRouteV2 route = getNormalRouteV2(V2RouteFile);
        String uuid="A";
        List<String> servers=Arrays.asList("11");
        String expectServer = "12";
        String server = route.locateNormalServer(uuid,servers);
        Assert.assertEquals(expectServer,server);
    }
    /**
     * 方法 locateNormalServer 无服务测试
     * NormalRouteV2内容：{"changeID":"123456","storageIndex":0,"secondID":"10","newSecondIDs":{"11":1,"12":2,"13":3},"version":"V2"}
     * uuid:"A"
     * 服务列表：["11"，"12","13"]
     * 预期 ：抛出异常IllegalArgumentException
     */
    @Test(expected=IllegalArgumentException.class)
    public void locateNormalServerExceptionTest(){
        NormalRouteV2 route = getNormalRouteV2(V2RouteFile);
        String uuid="A";
        List<String> servers=Arrays.asList("11","12","13");
        route.locateNormalServer(uuid,servers);

    }
}
