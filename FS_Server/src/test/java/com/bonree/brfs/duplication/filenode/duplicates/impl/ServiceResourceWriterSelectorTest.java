package com.bonree.brfs.duplication.filenode.duplicates.impl;

import com.bonree.brfs.common.utils.Pair;
import com.bonree.brfs.resourceschedule.model.LimitServerResource;
import com.bonree.brfs.resourceschedule.model.ResourceModel;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class ServiceResourceWriterSelectorTest{
    private static final Logger LOG = LoggerFactory.getLogger(ServiceResourceWriterSelectorTest.class);
    @Test
    public void testSelect(){
        String groupName = "a";
        int centSize = 100;
        ServiceResourceWriterSelector selector = new ServiceResourceWriterSelector(null,null,null,groupName,centSize);
        int num =3;
        List<ResourceModel> list = new ArrayList<>();
        List<Pair<String, Integer>> numList = new ArrayList<>();
        ResourceModel obj;
        for(int i = 0; i<num;i++){
            obj = new ResourceModel();
            obj.setHost("192.168.3."+i);
            obj.setServerId(String.valueOf(i));
            list.add(obj);
            numList.add(new Pair<>(String.valueOf(i),100));
        }
        Collection<ResourceModel> wins = selector.selectNode(null,list,numList,groupName,num);
        show(wins);

    }
    @Test
    public void testSelect01(){
        String groupName = "a";
        int centSize = 100;
        ServiceResourceWriterSelector selector = new ServiceResourceWriterSelector(null,null,null,groupName,centSize);
        int num =3;
        List<ResourceModel> list = new ArrayList<>();
        List<Pair<String, Integer>> numList = new ArrayList<>();
        ResourceModel obj;
        for(int i = 0; i<num;i++){
            obj = new ResourceModel();
            obj.setHost("192.168.3."+i);
            obj.setServerId(String.valueOf(i));
            list.add(obj);
            numList.add(new Pair<>(String.valueOf(i),100));
        }
        Collection<ResourceModel> wins = selector.selectNode(null,list,numList,groupName,2);
        show(wins);
    }
    @Test
    public void testSelect02(){
        String groupName = "a";
        int centSize = 100;
        ServiceResourceWriterSelector selector = new ServiceResourceWriterSelector(null,null,null,groupName,centSize);
        int num =2;
        List<ResourceModel> list = new ArrayList<>();
        List<Pair<String, Integer>> numList = new ArrayList<>();
        ResourceModel obj;
        for(int i = 0; i<num;i++){
            obj = new ResourceModel();
            obj.setHost("192.168.3."+i);
            obj.setServerId(String.valueOf(i));
            list.add(obj);
            numList.add(new Pair<>(String.valueOf(i),100));
        }
        Collection<ResourceModel> wins = selector.selectNode(null,list,numList,groupName,3);
        show(wins);
    }
    @Test
    public void testSelect03(){
        String groupName = "a";
        String sn = "11";
        int centSize = 100;
        ServiceResourceWriterSelector selector = new ServiceResourceWriterSelector(null,null,null,groupName,centSize);
        int num =2;
        List<ResourceModel> list = new ArrayList<>();
        List<Pair<String, Integer>> numList = new ArrayList<>();
        ResourceModel obj;
        for(int i = 0; i<num;i++){
            obj = new ResourceModel();
            obj.setHost("192.168.1.1");
            obj.setServerId(String.valueOf(i));
            list.add(obj);
            numList.add(new Pair<>(String.valueOf(i),100));
        }
        Collection<ResourceModel> wins = selector.selectNode(null,list,numList,sn,num);
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
        ServiceResourceWriterSelector selector = new ServiceResourceWriterSelector(null,null,limit,groupName,centSize);
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
}
