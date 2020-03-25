package com.bonree.brfs.duplication.filenode.duplicates.impl;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bonree.brfs.common.utils.Pair;
import com.bonree.brfs.resourceschedule.model.LimitServerResource;
import com.bonree.brfs.resourceschedule.model.ResourceModel;
import com.bonree.email.EmailPool;

public class MachineResourceWriterSelectorTest{
    private static final Logger LOG = LoggerFactory.getLogger(MachineResourceWriterSelectorTest.class);
    @Before
    public void initConf(){
        String resourcePath = Class.class.getResource("/").getPath()+"/server.properties";
        System.setProperty("configuration.file",resourcePath);
        EmailPool.getInstance();
    }
    @Test
    public void testSelect(){
        String groupName = "a";
        int centSize = 100;
        MachineResourceWriterSelector selector = new MachineResourceWriterSelector(null, null,null,groupName,5,centSize);
        int num =3;
        List<ResourceModel> list = new ArrayList<>();
        List<Pair<String, Integer>> numList = new ArrayList<>();
        ResourceModel obj;
        Map<String,ResourceModel> resourceModelMap = new HashMap<>();
        for(int i = 0; i<num;i++){
            obj = new ResourceModel();
            obj.setHost("192.168.3."+i);
            obj.setServerId(String.valueOf(i));
            list.add(obj);
            resourceModelMap.put(String.valueOf(i),obj);
            numList.add(new Pair<>(String.valueOf(i),100));
        }
        Collection<ResourceModel> wins = selector.selectNode(null,resourceModelMap,numList,groupName,num);
        show(wins);

    }
    @Test
    @SuppressWarnings("all")
    public void testSelect01(){
        String groupName = "a";
        int centSize = 100;
        MachineResourceWriterSelector selector = new MachineResourceWriterSelector(null, null,null,groupName,5,centSize);
        int num =3;
        List<ResourceModel> list = new ArrayList<>();
        List<Pair<String, Integer>> numList = new ArrayList<>();
        ResourceModel obj;
        Map<String,ResourceModel> resourceModelMap = new HashMap<>();
        for(int i = 0; i<num;i++){
            obj = new ResourceModel();
            obj.setHost("192.168.3."+i);
            obj.setServerId(String.valueOf(i));
            list.add(obj);
            numList.add(new Pair<>(String.valueOf(i),100));
            resourceModelMap.put(String.valueOf(i),obj);
        }
        Collection<ResourceModel> wins = selector.selectNode(null,resourceModelMap,numList,groupName,2);
        show(wins);
    }
    @Test
    public void testSelect02(){
        String groupName = "a";
        int centSize = 100;
        MachineResourceWriterSelector selector = new MachineResourceWriterSelector(null, null,null,groupName,5,centSize);
        int num =2;
        List<ResourceModel> list = new ArrayList<>();
        List<Pair<String, Integer>> numList = new ArrayList<>();
        ResourceModel obj;
        Map<String,ResourceModel> resourceModelMap = new HashMap<>();
        for(int i = 0; i<num;i++){
            obj = new ResourceModel();
            obj.setHost("192.168.3."+i);
            obj.setServerId(String.valueOf(i));
            list.add(obj);
            numList.add(new Pair<>(String.valueOf(i),100));
            resourceModelMap.put(String.valueOf(i),obj);

        }
        Collection<ResourceModel> wins = selector.selectNode(null,resourceModelMap,numList,groupName,3);
        show(wins);
    }
    @Test
    public void testSelect03(){
        String groupName = "a";
        String sn = "11";
        int centSize = 100;
        MachineResourceWriterSelector selector = new MachineResourceWriterSelector(null, null,null,groupName,5,centSize);
        int num =2;
        List<ResourceModel> list = new ArrayList<>();
        List<Pair<String, Integer>> numList = new ArrayList<>();
        ResourceModel obj;
        Map<String,ResourceModel> resourceModelMap = new HashMap<>();
        for(int i = 0; i<num;i++){
            obj = new ResourceModel();
            obj.setHost("192.168.1.1");
            obj.setServerId(String.valueOf(i));
            list.add(obj);
            numList.add(new Pair<>(String.valueOf(i),100));
            resourceModelMap.put(String.valueOf(i),obj);
        }
        Collection<ResourceModel> wins = selector.selectNode(null,resourceModelMap,numList,sn,num);
        show(wins);
    }

    @Test
    public void testFiler01(){
        String groupName = "a";
        int centSize = 100;
        LimitServerResource limit = new LimitServerResource();
        limit.setForceWriteValue(0.9);
        limit.setForceDiskRemainRate(0.01);
        limit.setDiskWriteValue(0.8);
        limit.setDiskRemainRate(0.05);
        limit.setRemainForceSize(20000000);
        limit.setRemainWarnSize(20000000);
        MachineResourceWriterSelector selector = new MachineResourceWriterSelector(null, null,limit,groupName,5,centSize);
        int num =1;
        List<ResourceModel> list = new ArrayList<>();
        List<Pair<String, Integer>> numList = new ArrayList<>();
        ResourceModel obj;
        String mount = "tmp";
        Map<String,Double> remainRateMap = new HashMap<>();
        remainRateMap.put("tmp",0.04);
        Map<String,Double> writValue = new HashMap<>();
        writValue.put("tmp",0.999);
        String sn = "sn";
        Map<String,String> snMap = new HashMap<>();
        snMap.put(sn,mount);
        for(int i = 0; i<num;i++){
            obj = new ResourceModel();
            obj.setHost("192.168.3."+i);
            obj.setDiskSize(20000000L);
            obj.setServerId(String.valueOf(i));
            obj.setLocalDiskRemainRate(remainRateMap);
            obj.setDiskWriteValue(writValue);
            obj.setStorageNameOnPartitionMap(snMap);
            list.add(obj);
            numList.add(new Pair<>(String.valueOf(i),100));
        }
        Collection<ResourceModel> wins = selector.filterService(list,sn);
        show(wins);
    }

    public void show(Collection<ResourceModel> resourceModels){
        for(ResourceModel r :resourceModels){
            LOG.info("{} {} done",r.getServerId(),r.getHost());
        }
    }
    @Test
    @SuppressWarnings("all")
    public void testSendEmail(){
        String groupName = "a";
        String sn = "11";
        int centSize = 100;
        MachineResourceWriterSelector selector = new MachineResourceWriterSelector(null, null,null,groupName,5,centSize);
        int num =2;
        List<ResourceModel> list = new ArrayList<>();
        List<Pair<String, Integer>> numList = new ArrayList<>();
        ResourceModel obj;
        Map<String,ResourceModel> resourceModelMap = new HashMap<>();
        for(int i = 0; i<num;i++){
            obj = new ResourceModel();
            obj.setHost("192.168.1.1");
            obj.setServerId(String.valueOf(i));
            list.add(obj);
            numList.add(new Pair<>(String.valueOf(i),100));
            resourceModelMap.put(String.valueOf(i),obj);
        }
        selector.sendSelectEmail(list,sn,num);
        try{
            Thread.sleep(1000L);
        } catch(InterruptedException e){
            e.printStackTrace();
        }
    }
}
