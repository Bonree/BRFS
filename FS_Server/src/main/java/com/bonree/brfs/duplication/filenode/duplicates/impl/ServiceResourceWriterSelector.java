package com.bonree.brfs.duplication.filenode.duplicates.impl;

import com.bonree.brfs.common.service.ServiceManager;
import com.bonree.brfs.common.utils.BrStringUtils;
import com.bonree.brfs.common.utils.Pair;
import com.bonree.brfs.duplication.datastream.connection.DiskNodeConnection;
import com.bonree.brfs.duplication.datastream.connection.DiskNodeConnectionPool;
import com.bonree.brfs.duplication.filenode.duplicates.ServiceSelector;
import com.bonree.brfs.resourceschedule.model.LimitServerResource;
import com.bonree.brfs.resourceschedule.model.ResourceModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * 服务写数据服务选择策略
 */
public class ServiceResourceWriterSelector implements ServiceSelector{
    private static final Logger LOG = LoggerFactory.getLogger(ServiceResourceWriterSelector.class);
    private DiskNodeConnectionPool connectionPool = null;
    private ServiceManager serviceManager = null;
    private String groupName = null;
    private int centSize = 1000;
    private LimitServerResource limit = null;
    public ServiceResourceWriterSelector(DiskNodeConnectionPool connectionPool,ServiceManager serviceManager,LimitServerResource limit, String groupName, int centSize){
        this.connectionPool = connectionPool;
        this.serviceManager = serviceManager;
        this.groupName = groupName;
        this.centSize = centSize;
        this.limit = limit;
    }
    @Override
    public Collection<ResourceModel> filterService(Collection<ResourceModel> resourceModels, String path){
        // 无资源
        if(resourceModels == null|| resourceModels.isEmpty()){
            return null;
        }
        Set<ResourceModel> wins = new HashSet<>();
        double diskLimit = this.limit.getForceDiskRemainRate();
        double diskWriteLimit = this.limit.getForceWriteValue();
        double diskRemainRate;
        double diskWritValue;
        for(ResourceModel resourceModel : resourceModels){
            diskRemainRate = resourceModel.getLocalDiskRemainRate(path);
            diskWritValue = resourceModel.getDiskWriteValue(path);
            if(diskRemainRate < diskLimit){
                LOG.warn("{}-{} disk remain {} will full refuse it ",resourceModel.getServerId(),path,diskRemainRate);
                continue;
            }
            if(diskWritValue > diskWriteLimit){
                LOG.warn("{}-{} disk remain {} will full refuse it ",resourceModel.getServerId(),path,diskWritValue);
                continue;
            }
            wins.add(resourceModel);
        }
        return wins;
    }

    @Override
    public Collection<ResourceModel> selector(Collection<ResourceModel> resources, String path, int num){
        if(resources == null || resources.isEmpty()){
            return  null;
        }
        List<Pair<String,Double>> values = new ArrayList<>();
        Pair<String,Double> tmpResource = null;
        double sum;
        String server;
        Map<String,ResourceModel> map = new HashMap<>();
        for(ResourceModel resource : resources){
            server = resource.getServerId();
            sum = resource.getDiskRemainValue(path) + resource.getDiskWriteValue(path);
            tmpResource = new Pair<>(server,sum);
            values.add(tmpResource);
            map.put(server,resource);
        }
        List<Pair<String, Integer>>intValues =  converDoublesToIntegers(values,centSize);

        List<ResourceModel> resourceModels = new ArrayList<>();
        String key = null;
        for(int i = 0; i< num;i++){
            key = WeightRandomPattern.getWeightRandom(intValues,new Random(),null);
            resourceModels.add(map.get(key));
        }
        return resourceModels;
    }


    /**
     * 概述：计算资源比值
     * @param servers
     * @return
     * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
     */
    private List<Pair<String, Integer>> converDoublesToIntegers(final List<Pair<String, Double>> servers, int preCentSize){
        List<Pair<String,Integer>> dents = new ArrayList<Pair<String,Integer>>();
        int total = 0;
        int value = 0;
        double sum = 0;
        int centSize = preCentSize<=0 ? 100 : preCentSize;
        for(Pair<String,Double> pair: servers) {
            sum +=pair.getSecond();
        }
        Pair<String,Integer> tmp = null;
        for(Pair<String,Double> ele : servers){
            tmp = new Pair<String, Integer>();
            tmp.setFirst(ele.getFirst());
            value = (int)(ele.getSecond()/sum* centSize);
            if(value == 0){
                value = 1;
            }
            tmp.setSecond(value);
            total += value;
            dents.add(tmp);
        }
        return dents;
    }
    @Override
    public List<Pair<String, Integer>> selectAvailableServers(int scene, String storageName, List<String> exceptionServerList, int centSize) throws Exception{
        return null;
    }

    @Override
    public List<Pair<String, Integer>> selectAvailableServers(int scene, int snId, List<String> exceptionServerList, int centSize) throws Exception{
        return null;
    }

    @Override
    public void setLimitParameter(LimitServerResource limits){
        this.limit = limits;
    }

    @Override
    public void update(ResourceModel resource){

    }

    @Override
    public void add(ResourceModel resources){

    }

    @Override
    public void remove(ResourceModel resource){

    }
}
