package com.bonree.brfs.duplication.filenode.duplicates.impl;

import com.bonree.brfs.common.service.ServiceManager;
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
 * 服务写数据服务选择策略 按磁盘剩余量选择服务，选择服务只会选择不同ip的，可能出现选出的节点数与需求的长度不一致
 */
public class MachineResourceWriterSelector implements ServiceSelector{
    private static final Logger LOG = LoggerFactory.getLogger(MachineResourceWriterSelector.class);
    private DiskNodeConnectionPool connectionPool = null;
    private ServiceManager serviceManager = null;
    private String groupName = null;
    private int centSize = 1000;
    private LimitServerResource limit = null;
    public MachineResourceWriterSelector(DiskNodeConnectionPool connectionPool,ServiceManager serviceManager,LimitServerResource limit, String groupName, int centSize){
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
        double diskWarnRemain = this.limit.getDiskRemainRate();
        double diskWarnWrite = this.limit.getDiskWriteValue();
        double diskRemainRate;
        double diskWritValue;
        for(ResourceModel resourceModel : resourceModels){
            diskRemainRate = resourceModel.getLocalDiskRemainRate(path);
            diskWritValue = resourceModel.getDiskWriteValue(path);
            if(diskRemainRate < diskLimit){
                LOG.warn("{}-{} disk remain {} will full refuse it ",resourceModel.getServerId(),path,diskRemainRate);
                continue;
            }
            if(diskRemainRate < diskWarnRemain ){
                LOG.warn("{}-{} disk remain {} will full warn !! ",resourceModel.getServerId(),path,diskRemainRate);
            }
//            if(diskWritValue > diskWriteLimit){
//                LOG.warn("{}-{} disk write {} will full refuse it ",resourceModel.getServerId(),path,diskWritValue);
//                continue;
//            }
            if(diskWritValue > diskWarnWrite){
                LOG.warn("{}-{} disk write {} will full warn ",resourceModel.getServerId(),path,diskWritValue);
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
        for(ResourceModel resource : resources){
            server = resource.getServerId();
            // 参数调整，disk写入io大的权重低
            sum = resource.getDiskRemainValue(path) + 1 - resource.getDiskWriteValue(path);
            tmpResource = new Pair<>(server,sum);
            values.add(tmpResource);
        }
        List<Pair<String, Integer>>intValues =  converDoublesToIntegers(values,centSize);

        return selectNode(this.connectionPool,resources,intValues,this.groupName,num);
    }

    /**
     * 服务选择
     * @param resources
     * @param intValues
     * @param num
     * @return
     */
    public Collection<ResourceModel> selectNode(DiskNodeConnectionPool pool,Collection<ResourceModel> resources,List<Pair<String, Integer>>intValues,String groupName,int num){
        Map<String,ResourceModel> map = new HashMap<>();
        for(ResourceModel resource : resources){
            map.put(resource.getServerId(),resource);
        }
        List<ResourceModel> resourceModels = new ArrayList<>();
        String key;
        String ip;
        ResourceModel tmp;
        DiskNodeConnection conn;
        //ip选中
        Set<String> ips = new HashSet<>();
        List<String> uneedServices = new ArrayList<>();
        int tSize = resources.size();
        while(resourceModels.size() != num && resourceModels.size() !=tSize && uneedServices.size() !=tSize){
            key = WeightRandomPattern.getWeightRandom(intValues,new Random(),uneedServices);
            tmp = map.get(key);
            ip = tmp.getHost();
            if(pool != null){
                conn = pool.getConnection(groupName,key);
                if(conn == null || !conn.isValid()){
                    LOG.warn("{} :[{}({})]is unused !!",groupName,key,ip);
                    uneedServices.add(key);
                    continue;
                }
            }
            // 不同ip的添加
            if(ips.add(ip)){
                resourceModels.add(tmp);
            }else{
                LOG.info("{} is selectd !! get next", ip);
            }
            uneedServices.add(tmp.getServerId());
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
