package com.bonree.brfs.duplication.filenode.duplicates.impl;

import com.bonree.brfs.common.utils.Pair;
import com.bonree.brfs.configuration.Configs;
import com.bonree.brfs.configuration.units.ResourceConfigs;
import com.bonree.brfs.duplication.datastream.connection.DiskNodeConnection;
import com.bonree.brfs.duplication.datastream.connection.DiskNodeConnectionPool;
import com.bonree.brfs.duplication.filenode.FileNodeStorer;
import com.bonree.brfs.duplication.filenode.duplicates.ServiceSelector;
import com.bonree.brfs.email.EmailPool;
import com.bonree.brfs.resourceschedule.model.LimitServerResource;
import com.bonree.brfs.resourceschedule.model.ResourceModel;
import com.bonree.mail.worker.MailWorker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * 服务写数据服务选择策略 按磁盘剩余量选择服务，选择服务只会选择不同ip的，可能出现选出的节点数与需求的长度不一致
 */
public class MachineResourceWriterSelector implements ServiceSelector{
    private static final Logger LOG = LoggerFactory.getLogger(MachineResourceWriterSelector.class);
    //记录少于服务时
    private static long preTime = 0L;
    //记录重复数值的
    private static long repeatTime = 0L;
    private static long INVERTTIME = Configs.getConfiguration().GetConfig(ResourceConfigs.CONFIG_RESOURCE_EMAIL_INVERT)*1000;
    private DiskNodeConnectionPool connectionPool;
    private String groupName;
    private int centSize;
    private LimitServerResource limit;
    private FileNodeStorer storer;
    private long fileSize = 0;
    public MachineResourceWriterSelector(DiskNodeConnectionPool connectionPool,FileNodeStorer storer, LimitServerResource limit, String groupName, long fileSize,int centSize){
        this.connectionPool = connectionPool;
        this.storer =storer;
        this.groupName = groupName;
        this.centSize = centSize;
        this.limit = limit;
        this.fileSize = fileSize;
    }
    @Override
    public Collection<ResourceModel> filterService(Collection<ResourceModel> resourceModels, String sn){
        // 无资源
        if(resourceModels == null|| resourceModels.isEmpty()){
            LOG.warn("no resource to selector");
            return null;
        }
        Set<ResourceModel> wins = new HashSet<>();
        long diskRemainSize;
        int numSize =this.storer == null ? 0: this.storer.fileNodeSize();

        List<ResourceModel> washroom = new ArrayList<>();
        // 将已经满足条件的服务过滤
        for(ResourceModel wash : resourceModels){
            diskRemainSize = wash.getLocalRemainSizeValue(sn);
            if(diskRemainSize < this.limit.getRemainForceSize()){
                LOG.warn("First sn: {} {}({}), path: {} remainsize: {}, force:{} !! will refused",
                              sn,wash.getServerId(),wash.getHost(),wash.getMountedPoint(sn),diskRemainSize,this.limit.getRemainForceSize());
                continue;
            }
            washroom.add(wash);
        }
        if(washroom.isEmpty()){
            return  null;
        }
        int size = washroom.size();
        // 预测值，假设现在所有正在写的文件大小为0，并且每个磁盘节点都写入。通过现有写入的文件的数×配置的文件大小即可得单个数据节点写入数据的大小
        long writeSize = numSize * fileSize;
        for(ResourceModel resourceModel : washroom){
            diskRemainSize = resourceModel.getLocalRemainSizeValue(sn) - writeSize;
            if(diskRemainSize < this.limit.getRemainForceSize()){
                LOG.warn("Second sn: {} {}({}), path: {} remainsize: {}, force:{} !! will refused",sn,resourceModel.getServerId(),resourceModel.getHost(),resourceModel.getMountedPoint(sn),diskRemainSize,this.limit.getRemainForceSize());
                continue;
            }
            if(diskRemainSize <this.limit.getRemainWarnSize()){
                LOG.warn("sn: {} {}({}), path: {} remainsize: {}, force:{} !! will full",sn,resourceModel.getServerId(),resourceModel.getHost(),resourceModel.getMountedPoint(sn),diskRemainSize,this.limit.getRemainForceSize());
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
        // 如果可选服务少于需要的，发送报警邮件
        int resourceSize = resources.size();
        boolean lessFlag = resourceSize < num;
        if(lessFlag){
            long currentTime = System.currentTimeMillis();
            // 控制邮件发送的间隔，减少不必要的
            if(currentTime - preTime > INVERTTIME){
                sendSelectEmail(resources,path,num);
            }
            return resources;
        }
        // 转换为Map
        Map<String,ResourceModel> map = convertResourceMap(resources);
        // 转换为权重值
        List<Pair<String, Integer>>intValues =  covertValues(resources,path,centSize);
        List<ResourceModel> wins = selectNode(this.connectionPool,map,intValues,this.groupName,num);
        int winSize = wins.size();
        // 若根据ip分配的个数满足要求，则返回该集合，不满足则看是否为
        if(winSize == num){
            return  wins;
        }
        LOG.warn("will select service in same ip !!!");
        Set<String> sids = selectWins(wins);
        // 二次选择服务
        int sSize = resourceSize > num ? num - winSize : resourceSize - winSize;
        Collection<ResourceModel> sWins = selectRandom(this.connectionPool,map,sids,intValues,groupName,path,sSize);
        wins.addAll(sWins);
        // 若依旧不满足则发送邮件
        sendSelectEmail(wins,path,num);
        return wins;

    }

    /**
     * 发送选择邮件
     * @param resourceModels
     * @param sn
     * @param num
     */
    public void sendSelectEmail(Collection<ResourceModel> resourceModels,String sn,int num){
        StringBuilder messageBuilder = new StringBuilder();
        messageBuilder.append("sr:[")
                .append(sn).
                append("] 写入可供选择的服务少于需要的!! 可用服务 ")
                .append(resourceModels.size()).append(", 需要 ")
                .append(num).append("(文件分布见上下文表格)");
        Map<String,String> map = new HashMap<>();
        String part;
        String key;
        for(ResourceModel resource : resourceModels){
            key = resource.getServerId()+"("+resource.getHost()+")";
            part = sn+"[path:"+resource.getMountedPoint(sn)+", remainSize: "+resource.getLocalRemainSizeValue(sn)+"b]";
            map.put(key,part);
        }
        EmailPool emailPool = EmailPool.getInstance();
        MailWorker.Builder builder = MailWorker.newBuilder(emailPool.getProgramInfo());
        builder.setModel(this.getClass().getSimpleName()+"服务选择");
        builder.setMessage(messageBuilder.toString());
        if(!map.isEmpty()){
            builder.setVariable(map);
        }
        emailPool.sendEmail(builder);
    }
    /**
     * 随机选择
     * @param pool
     * @param map
     * @param sids
     * @param intValues
     * @param groupName
     * @param sn
     * @param num
     * @return
     */
    public Collection<ResourceModel> selectRandom(DiskNodeConnectionPool pool, Map<String,ResourceModel> map,Set<String> sids,List<Pair<String,Integer>> intValues,String groupName,String sn,int num){
        List<ResourceModel> resourceModels = new ArrayList<>();
        String key;
        String ip;
        ResourceModel tmp;
        DiskNodeConnection conn;
        //ip选中优先选择
        int tSize = map.size();
        // 按资源选择
        Random random = new Random();
        boolean sendFlag = System.currentTimeMillis() - repeatTime > INVERTTIME;
        repeatTime = sendFlag ? System.currentTimeMillis() : repeatTime;
        while(resourceModels.size() != num && resourceModels.size() !=tSize && sids.size() !=tSize){
            key = WeightRandomPattern.getWeightRandom(intValues,random,sids);
            tmp = map.get(key);
            ip = tmp.getHost();
            if(pool != null){
                conn = pool.getConnection(groupName,key);
                if(conn == null || !conn.isValid()){
                    LOG.warn("{} :[{}({})]is unused !!",groupName,key,ip);
                    sids.add(key);
                    continue;
                }
            }

            if(sendFlag){
                EmailPool emailPool = EmailPool.getInstance();
                MailWorker.Builder builder = MailWorker.newBuilder(emailPool.getProgramInfo())
                        .setModel(this.getClass().getSimpleName()+"服务选择")
                        .setMessage("sr ["+sn+"]即将 在 "+key+"("+ip+") 服务 写入重复数据");
                emailPool.sendEmail(builder);
            }
            resourceModels.add(tmp);
            sids.add(tmp.getServerId());
        }
        return resourceModels;
    }
    /**
     * 获取已选择服务的services
     * @param wins
     * @return
     */
    public Set<String> selectWins(List<ResourceModel> wins){
        Set<String> set = new HashSet<>();
        for(ResourceModel resourceModel : wins){
            set.add(resourceModel.getServerId());
        }
        return set;
    }
    public List<Pair<String,Integer>> covertValues(Collection<ResourceModel> resources, String path, int centSize){
        List<Pair<String,Double>> values = new ArrayList<>();
        Pair<String,Double> tmpResource;
        double sum;
        String server;
        for(ResourceModel resource : resources){
            server = resource.getServerId();
            // 参数调整，disk写入io大的权重低
            sum = resource.getDiskRemainValue(path) + 1 - resource.getDiskWriteValue(path);
            tmpResource = new Pair<>(server,sum);
            values.add(tmpResource);
        }
        return converDoublesToIntegers(values,centSize);
    }
    /**
     * 服务选择
     * @param intValues
     * @param num
     * @return
     */
    public List<ResourceModel> selectNode(DiskNodeConnectionPool pool,Map<String,ResourceModel> map,List<Pair<String, Integer>>intValues,String groupName,int num){

        List<ResourceModel> resourceModels = new ArrayList<>();
        String key;
        String ip;
        ResourceModel tmp;
        DiskNodeConnection conn;
        //ip选中优先选择
        Set<String> ips = new HashSet<>();
        List<String> uneedServices = new ArrayList<>();
        int tSize = map.size();
        // 按资源选择
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
     * 转换为map
     * @param resources
     * @return
     */
    public Map<String,ResourceModel> convertResourceMap(Collection<ResourceModel> resources){
        Map<String,ResourceModel> map = new HashMap<>();
        for(ResourceModel resource : resources){
            map.put(resource.getServerId(),resource);
        }
        return map;
    }

    /**
     * 概述：计算资源比值
     * @param servers
     * @return
     * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
     */
    private List<Pair<String, Integer>> converDoublesToIntegers(final List<Pair<String, Double>> servers, int preCentSize){
        List<Pair<String,Integer>> dents = new ArrayList<Pair<String,Integer>>();
        int value;
        double sum = 0;
        int centSize = preCentSize<=0 ? 100 : preCentSize;
        for(Pair<String,Double> pair: servers) {
            sum +=pair.getSecond();
        }
        Pair<String,Integer> tmp;
        for(Pair<String,Double> ele : servers){
            tmp = new Pair<>();
            tmp.setFirst(ele.getFirst());
            value = (int)(ele.getSecond()/sum* centSize);
            if(value == 0){
                value = 1;
            }
            tmp.setSecond(value);
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
