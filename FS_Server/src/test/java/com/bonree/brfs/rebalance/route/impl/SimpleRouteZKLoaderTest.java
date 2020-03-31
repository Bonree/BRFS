package com.bonree.brfs.rebalance.route.impl;

import com.bonree.brfs.common.rebalance.Constants;
import com.bonree.brfs.common.rebalance.route.NormalRouteInterface;
import com.bonree.brfs.common.rebalance.route.VirtualRoute;
import com.bonree.brfs.common.rebalance.route.impl.SuperNormalRoute;
import com.bonree.brfs.common.utils.JsonUtils;
import com.bonree.brfs.common.zookeeper.curator.CuratorClient;
import com.fasterxml.jackson.core.type.TypeReference;
import org.apache.commons.io.FileUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryNTimes;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;

/*******************************************************************************
 * 版权信息： 北京博睿宏远数据科技股份有限公司
 * Copyright (c) 2007-2020 北京博睿宏远数据科技股份有限公司，Inc. All Rights Reserved.
 * @date 2020年03月31日 09:52:05
 * @author: <a href=mailto:zhucg@bonree.com>朱成岗</a>
 * @description:
 ******************************************************************************/

public class SimpleRouteZKLoaderTest {
    private static String resourcePath = SimpleRouteZKLoaderTest.class.getResource("/Routes/ZookeeperRoute").getPath();
    private static CuratorClient zkClient = null;
    private static String V1_ARRAY="V1Array.json";
    private static String V2_ARRAY="V2Array.json";
    private static String MUL_ARRAY="MULArray.json";
    private static String VIRTUAL_ARRAY="VirtualArray.json";
    private static String V1_ROUTE_BAS_PATH="/brfsDevTest/route/V1";
    private static String V2_ROUTE_BAS_PATH="/brfsDevTest/route/V2";
    private static String MUL_VERSION_ROUTE_BAS_PATH="/brfsDevTest/route/Mul";
    public static String ZK_ADDRESS= "192.168.101.87:2181";
    private static CuratorFramework client = null;
    private static int SR_ID=0;
    /**
     * 检查测试资源是否存在，若不存在，则测试失败
     */
    @Before
    public void checkLoad()throws Exception{
        load();
    }
    public void load(){
        //1. 初始化zk客户端
        zkClient = CuratorClient.getClientInstance(ZK_ADDRESS);
        client = CuratorFrameworkFactory.newClient(ZK_ADDRESS,new RetryNTimes(10,1000));
        try {
            client.start();
            zkClient.blockUntilConnected(10, TimeUnit.SECONDS);
            client.blockUntilConnected();
        } catch (InterruptedException e) {
            Assert.fail("zookeeper is invaild ! address: "+ZK_ADDRESS);
        }
        // 2.创建虚拟路由规则
        createZkVirtualRouteData(zkClient,V1_ROUTE_BAS_PATH,VIRTUAL_ARRAY);
        createZkVirtualRouteData(zkClient,V2_ROUTE_BAS_PATH,VIRTUAL_ARRAY);
        createZkVirtualRouteData(zkClient,MUL_VERSION_ROUTE_BAS_PATH,VIRTUAL_ARRAY);
        // 3. 创建V1版本的路由规则
        createZkNormalData(zkClient,V1_ROUTE_BAS_PATH,V1_ARRAY);
        createZkNormalData(zkClient,V2_ROUTE_BAS_PATH,V2_ARRAY);
        createZkNormalData(zkClient,MUL_VERSION_ROUTE_BAS_PATH,MUL_ARRAY);
    }
    /**
     * 在zookeeper 创建路由规则
     * @param client
     * @param zkNode
     * @param fileName
     */
    public void createZkNormalData(CuratorClient client,String zkNode,String fileName){
        if(!client.checkExists(zkNode)){
            client.createPersistent(zkNode,true);
        }
        List<SuperNormalRoute> routes = readNormalRoute(fileName);
        int srId = -1;
        String changeID = null;
        String nZnode = null;
        for(SuperNormalRoute route : routes){
            srId =route.getStorageIndex();
            changeID = route.getChangeID();
            nZnode = zkNode+ Constants.SEPARATOR+Constants.NORMAL_ROUTE+ Constants.SEPARATOR+srId+Constants.SEPARATOR+changeID;
            if(!client.checkExists(nZnode)){
                client.createPersistent(nZnode,true, JsonUtils.toJsonBytesQuietly(route));
            }
        }
    }

    /**
     * 在zookeeper 创建虚拟serverId的规则
     * @param client
     * @param zkNode
     * @param fileName
     */
    public void createZkVirtualRouteData(CuratorClient client,String zkNode,String fileName){
        if(!client.checkExists(zkNode)){
            client.createPersistent(zkNode,true);
        }
        List<VirtualRoute> routes = readVirtualRoute(fileName);
        int srId = -1;
        String changeID = null;
        String nZnode = null;
        for(VirtualRoute route : routes){
            srId =route.getStorageIndex();
            changeID = route.getChangeID();
            nZnode = zkNode+Constants.SEPARATOR+Constants.VIRTUAL_ROUTE+ Constants.SEPARATOR+srId+Constants.SEPARATOR+changeID;
            if(!client.checkExists(nZnode)){
                client.createPersistent(nZnode,true,JsonUtils.toJsonBytesQuietly(route));
            }
        }
    }

    /**
     * 读取正常路由规则
     * @param fileName
     * @return
     */
    public List<SuperNormalRoute> readNormalRoute (String fileName){
        List<SuperNormalRoute> list = null;
        byte[] data = null;
        try {
            data = readBytesFromFile(fileName);
            list = JsonUtils.toObject(data, new TypeReference<List<SuperNormalRoute>>(){});
        } catch (JsonUtils.JsonException e) {
            Assert.fail("Deserlize byte[] fail "+data == null ? "" : new String(data));
        }
        return list;
    }

    /**
     * 读取虚拟路由规则
     * @param fileName
     * @return
     */
    public List<VirtualRoute> readVirtualRoute(String fileName){
        List<VirtualRoute> list = null;
        byte[] data = null;
        try {
            data = readBytesFromFile(fileName);
            list = JsonUtils.toObject(data, new TypeReference<List<VirtualRoute>>(){});
        } catch (JsonUtils.JsonException e) {
            Assert.fail("Deserlize byte[] fail "+data == null ? "" : new String(data));
        }
        return list;
    }

    /**
     * 读取文件信息
     * @param fileName
     * @return
     */
    public byte[] readBytesFromFile(String fileName) {
        String filePath = resourcePath+ File.separator+fileName;
        try {
            return FileUtils.readFileToByteArray(new File(filePath));
        } catch (IOException e) {
            Assert.fail("read file happen error !! file:"+filePath);
        }
        return null;
    }
    @Test
    public void loadNormalV1Test()throws Exception{
        SimpleRouteZKLoader loader = new SimpleRouteZKLoader(client,V1_ROUTE_BAS_PATH);
        Collection<NormalRouteInterface> list = loader.loadNormalRoutes(SR_ID);
        Assert.assertNotNull(list);
    }
    @Test
    public void loadVirtualV1Test()throws Exception{
        SimpleRouteZKLoader loader = new SimpleRouteZKLoader(client,V1_ROUTE_BAS_PATH);
        Collection<VirtualRoute> list = loader.loadVirtualRoutes(SR_ID);
        Assert.assertNotNull(list);
    }

    @Test
    public void loadNormalV2Test()throws Exception{
        SimpleRouteZKLoader loader = new SimpleRouteZKLoader(client,V2_ROUTE_BAS_PATH);
        Collection<NormalRouteInterface> list = loader.loadNormalRoutes(SR_ID);
        Assert.assertNotNull(list);
    }
    @Test
    public void loadVirtualV2Test()throws Exception{
        SimpleRouteZKLoader loader = new SimpleRouteZKLoader(client,V2_ROUTE_BAS_PATH);
        Collection<VirtualRoute> list = loader.loadVirtualRoutes(SR_ID);
        Assert.assertNotNull(list);
    }
    @Test
    public void loadNormalMULTest()throws Exception{
        SimpleRouteZKLoader loader = new SimpleRouteZKLoader(client,MUL_VERSION_ROUTE_BAS_PATH);
        Collection<NormalRouteInterface> list = loader.loadNormalRoutes(SR_ID);
        Assert.assertNotNull(list);
    }
    @Test
    public void loadVirtualMULTest()throws Exception{
        SimpleRouteZKLoader loader = new SimpleRouteZKLoader(client,MUL_VERSION_ROUTE_BAS_PATH);
        Collection<VirtualRoute> list = loader.loadVirtualRoutes(SR_ID);
        Assert.assertNotNull(list);
    }
    @After
    public void close(){
        if(zkClient !=null){
            zkClient.close();
        }
        if(client!=null){
            client.close();
        }
    }
}