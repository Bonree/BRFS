package com.bonree.brfs.identification.impl;

import com.bonree.brfs.common.rebalance.Constants;
import com.bonree.brfs.common.rebalance.route.NormalRouteInterface;
import com.bonree.brfs.common.utils.BrStringUtils;
import com.bonree.brfs.identification.SecondMaintainerInterface;
import com.bonree.brfs.rebalance.route.factory.SingleRouteFactory;
import com.bonree.brfs.server.identification.LevelServerIDGen;
import org.apache.commons.lang3.StringUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.utils.ZKPaths;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.*;

/*******************************************************************************
 * 版权信息： 北京博睿宏远数据科技股份有限公司
 * Copyright (c) 2007-2020 北京博睿宏远数据科技股份有限公司，Inc. All Rights Reserved.
 * @date 2020年04月01日 14:35:45
 * @author: <a href=mailto:zhucg@bonree.com>朱成岗</a>
 * @description: 注册二级serverid维护类
 ******************************************************************************/

public class SimpleSecondMaintainer implements SecondMaintainerInterface {
    private static final Logger LOG = LoggerFactory.getLogger(SimpleSecondMaintainer.class);
    private LevelServerIDGen secondIdWorker;
    private CuratorFramework client = null;
    private String secondBasePath;
    private String routeBasePath;
    private String firstServer;
    private Map<String,String> cache = new HashMap<>();
    private Collection<String> localPartitionId = null;
    public SimpleSecondMaintainer(CuratorFramework client, String secondBasePath, String routeBasePath, String secondIdSeqPath,String firstServer) {
        this.client = client;
        this.secondBasePath = secondBasePath;
        this.routeBasePath = routeBasePath;
        this.secondIdWorker = new SecondServerIDGenImpl(this.client,secondIdSeqPath);
        this.firstServer = firstServer;
        loadLocal();
    }

    /**
     * 注册二级serverid，当一级serverid为空时则默认注册本机磁盘id
     * @param firstServer
     * @param partitionId
     * @param storageId
     * @return
     */
    @Override
    public String registerSecondId(String firstServer, String partitionId, int storageId) {
        String key = getKey(partitionId,storageId+"");
        String secondId = cache.get(key);
        if(StringUtils.isEmpty(secondId)){
            String content = StringUtils.isEmpty(firstServer) ? this.firstServer :firstServer;
            secondId = createSecondId(partitionId,content,storageId);
            cache.put(key,secondId);
        }
        return secondId;
    }
    public Collection<String> registerSecondIdLocalServer(int storageId){
        return registerSecondIdBatch(this.localPartitionId,this.firstServer,storageId);
    }
    public Collection<String> registerSecondIdBatch(Collection<String> partitionIds,String firstServer, int storageId){
        if(partitionIds == null || partitionIds.isEmpty()){
            return null;
        }
        List<String> secondIds = new ArrayList<>();
        for(String partitionId : partitionIds){
            String secondId = registerSecondId(firstServer,partitionId,storageId);
            secondIds.add(secondId);
        }
        return secondIds;
    }
    private String createSecondId(String partitionId,String content,int storageId){
        String secondId = secondIdWorker.genLevelID();
        addPartitionRelation(content,partitionId);
        String sPath = ZKPaths.makePath(secondBasePath,partitionId,storageId+"");
        try {
            if(client.checkExists().forPath(sPath) ==null){
                client.create().creatingParentsIfNeeded().forPath(sPath,secondId.getBytes(StandardCharsets.UTF_8));
            }else{
                client.setData().forPath(sPath,secondId.getBytes(StandardCharsets.UTF_8));
            }
        } catch (Exception ignore) {
        }
        try {
            byte[] sdata = client.getData().forPath(sPath);
            if(sdata == null || sdata.length == 0){
                throw new RuntimeException("register secondid fail ! second id is empty !!");
            }
            String zSecond = new String(sdata,StandardCharsets.UTF_8);
            if(!secondId.equals(zSecond)){
                throw new RuntimeException("register secondid fail ! apply secondid:["+secondId+"], zkSecondId:["+zSecond+"]content not same  !!");
            }
        } catch (Exception e) {
            throw new RuntimeException("partitionId:["+partitionId+"],storageId:["+storageId+"] register secondid happen error !",e);
        }
        return secondId;
    }
    @Override
    public boolean unregisterSecondId(String firstServer,String partitionId, int storageId) {
        return unRegisterSecondId(partitionId,storageId);
    }
    public boolean unRegisterSecondIdLocalServer(int storageid){
        return unRegisterSecondIdBatch(this.localPartitionId,storageid);
    }
    public boolean unRegisterSecondIdBatch(Collection<String> localPartitionIds,int storageId){
        if(localPartitionIds == null || localPartitionIds.isEmpty()){
            return true;
        }
        boolean status = true;
        for(String partitionId : localPartitionIds){
            status &=unRegisterSecondId(partitionId,storageId);
        }
        return status;
    }
    public boolean unRegisterSecondId(String partitionId,int storageId){
        String key = getKey(partitionId,storageId+"");
        String serverId = cache.remove(key);
        if(serverId == null) {
            return true;
        }
        try {
            String sPath = ZKPaths.makePath(secondBasePath,partitionId,String.valueOf(storageId));
            client.delete().quietly().forPath(sPath);
            return true;
        } catch (Exception e) {
            LOG.error("can not delete second server id node", e);
        }
        return false;
    }

    @Override
    public boolean isValidSecondId(String secondId, int storageId) {
        try {
            String normalPath = ZKPaths.makePath(routeBasePath, Constants.NORMAL_ROUTE);
            String siPath = ZKPaths.makePath(normalPath, secondId);
            if (client.checkExists().forPath(normalPath)!=null && client.checkExists().forPath(siPath)!=null) {
                List<String> routeNodes = client.getChildren().forPath(siPath);
                for (String routeNode : routeNodes) {
                    String routePath = ZKPaths.makePath(siPath, routeNode);
                    byte[] data = client.getData().forPath(routePath);
                    NormalRouteInterface normalRoute = SingleRouteFactory.createRoute(data);
                    if (normalRoute.getBaseSecondId().equals(secondId)) {
                        return true;
                    }
                }
            }
        } catch (Exception e) {
            LOG.error("check secondId[{}:{}] happen error {}",secondId,storageId,e);
        }
        return false;
    }

    @Override
    public void addPartitionRelation(String firstServer, String partitionId) {
        String pPath = ZKPaths.makePath(secondBasePath,partitionId);
        try {
            if(client.checkExists().forPath(pPath) == null){
                client.create().creatingParentsIfNeeded().forPath(pPath,firstServer.getBytes(StandardCharsets.UTF_8));
            }
        } catch (Exception ignore) {
        }
        try {
            byte[] fdata = client.getData().forPath(pPath);
            if(fdata == null ||fdata.length == 0){
                throw new RuntimeException("add partition relationShip fail! first server id is empty !!");
            }
            String zFirst = new String(fdata,StandardCharsets.UTF_8);
            if(!firstServer.equals(zFirst)){
                throw new RuntimeException("add partition relationShip fail ! firstServerId:["+firstServer+"], zk first:["+zFirst+"]content not same  !!");
            }
        } catch (Exception e) {
            throw new RuntimeException("add partition relationShip fail !! ["+partitionId+"],firstserver:["+firstServer+"] ",e);

        }
    }

    @Override
    public boolean removePartitionRelation(String partitionid) {
        try {
            String pPath = ZKPaths.makePath(this.secondBasePath,partitionid);
            client.delete().quietly().forPath(pPath);
            return true;
        } catch (Exception e) {
            LOG.error("can not delete second server id node", e);
        }
        return false;
    }

    private void loadLocal() {
        try {
            if(client.checkExists().forPath(secondBasePath) == null){
                client.create().creatingParentsIfNeeded().forPath(secondBasePath);
            }
            List<String> validPartitions = client.getChildren().forPath(secondBasePath);
            this.localPartitionId = getValidPartitions(this.firstServer);
            // 无数据则返回
            if(validPartitions.isEmpty()){
                return;
            }
            // 此处需要进行判断是否过期
            for (String partition : validPartitions) {
               loadCache(partition);
            }
            LOG.info("load {} second server ID cache:{}", firstServer,cache);
        }catch (Exception e) {
            LOG.error("load self second server ID error!!!",e);
        }
    }
    private void loadCache(String partition)throws Exception{
        String pPath = ZKPaths.makePath(secondBasePath,partition);
        List<String> storageIndeies = client.getChildren().forPath(pPath);
        if(storageIndeies ==null|| storageIndeies.isEmpty()){
            return;
        }
        for (String si : storageIndeies) {
            String node = ZKPaths.makePath(pPath, si);
            byte[] data = client.getData().forPath(node);
            String serverID = BrStringUtils.fromUtf8Bytes(data);
            if (isValidSecondId(si, Integer.parseInt(si))) { // 判断secondServerID是否过期，过期需要重新生成
                serverID = secondIdWorker.genLevelID();
                client.setData().forPath(node, serverID.getBytes(StandardCharsets.UTF_8));
            }
            cache.put(getKey(partition,si), serverID);
        }
    }

    @NotNull
    private List<String> getValidPartitions(String firstServer) throws Exception {
        List<String> partitions = client.getChildren().forPath(secondBasePath);
        List<String> validPartitions = new ArrayList<>();
        for(String partition : partitions){
            String pPath = ZKPaths.makePath(secondBasePath,partition);
            byte[] data = client.getData().forPath(pPath);
            // 无效的节点跳过
            if(data == null || data.length ==0){
                LOG.warn("secondId find invalid node [{}]",pPath);
                continue;
            }
            String tmpF = new String(data, StandardCharsets.UTF_8);
            if(firstServer.equals(tmpF)){
                validPartitions.add(partition);
            }
        }
        return validPartitions;
    }

    private String getKey(String partition,String storageId){
        return partition+":"+storageId;
    }
}
