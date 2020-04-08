package com.bonree.brfs.identification.impl;

import com.bonree.brfs.common.rebalance.Constants;
import com.bonree.brfs.common.rebalance.route.NormalRouteInterface;
import com.bonree.brfs.identification.SecondIdsInterface;
import com.bonree.brfs.identification.SecondMaintainerInterface;
import com.bonree.brfs.rebalance.route.factory.SingleRouteFactory;
import com.bonree.brfs.identification.LevelServerIDGen;
import org.apache.commons.lang3.StringUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.utils.ZKPaths;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.*;

/*******************************************************************************
 * 版权信息： 北京博睿宏远数据科技股份有限公司
 * Copyright (c) 2007-2020 北京博睿宏远数据科技股份有限公司，Inc. All Rights Reserved.
 * @date 2020年04月01日 14:35:45
 * @author: <a href=mailto:zhucg@bonree.com>朱成岗</a>
 * @description: 注册二级serverid维护类 datanode服务独享，client 需要使用
 ******************************************************************************/

public class SimpleSecondMaintainer implements SecondMaintainerInterface {
    private static final Logger LOG = LoggerFactory.getLogger(SimpleSecondMaintainer.class);
    private LevelServerIDGen secondIdWorker;
    private CuratorFramework client = null;
    private String secondBasePath;
    private String routeBasePath;
    private SecondIdsInterface secondIds;
    public SimpleSecondMaintainer(CuratorFramework client, String secondBasePath, String routeBasePath, String secondIdSeqPath) {
        this.client = client;
        this.secondBasePath = secondBasePath;
        this.routeBasePath = routeBasePath;
        this.secondIdWorker = new SecondServerIDGenImpl(this.client,secondIdSeqPath);
        this.secondIds = new RetryNTimesSecondIDShip(client,secondBasePath,3,100);
    }

    /**
     * 注册二级serverid，
     * @param firstServer
     * @param partitionId
     * @param storageId
     * @return
     */
    @Override
    public String registerSecondId(String firstServer, String partitionId, int storageId) {
        if(StringUtils.isEmpty(firstServer)){
            return null;
        }
        String secondId = this.secondIds.getSecondId(partitionId,storageId);
        if(StringUtils.isEmpty(secondId)){
            secondId = createSecondId(partitionId,firstServer,storageId);
        }
        return secondId;
    }

    @Override
    public Collection<String> registerSecondIds(String firstServer, int storageId) {
        List<String> partitionIds = null;
        try {
            partitionIds = getValidPartitions(firstServer);
        } catch (Exception e) {
            LOG.error("storage[{}] load firstServer[{}] partitionIds happen error ",storageId,firstServer,e);
        }
        if(partitionIds == null || partitionIds.isEmpty()){
            return null;
        }
        return registerSecondIdBatch(partitionIds,firstServer,storageId);
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
    public boolean unregisterSecondId(String partitionId, int storageId) {
        String serverId = this.secondIds.getSecondId(partitionId,storageId);
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
    public boolean unregisterSecondIds(String firstServer, int storageId) {
        List<String> partitionIds = null;
        try {
            partitionIds = getValidPartitions(firstServer);
        } catch (Exception e) {
            LOG.error("storage[{}] load firstServer[{}] partitionIds happen error ",storageId,firstServer,e);
        }
        if(partitionIds == null || partitionIds.isEmpty()){
            return true;
        }
        return unRegisterSecondIdBatch(partitionIds,storageId);
    }

    public boolean unRegisterSecondIdBatch(Collection<String> localPartitionIds,int storageId){
        if(localPartitionIds == null || localPartitionIds.isEmpty()){
            return true;
        }
        boolean status = true;
        for(String partitionId : localPartitionIds){
            status &=unregisterSecondIds(partitionId,storageId);
        }
        return status;
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
    public void addAllPartitionRelation(Collection<String> partitionIds, String firstServer) {
        if(partitionIds == null || partitionIds.isEmpty() ||StringUtils.isEmpty(firstServer)){
            return;
        }
        for(String partitionId : partitionIds){
            addPartitionRelation(firstServer,partitionId);
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

    @Override
    public boolean removeAllPartitionRelation(Collection<String> partitionIds) {
        if(partitionIds == null || partitionIds.isEmpty()){
            return true;
        }
        boolean status = true;
        for(String partitionId : partitionIds){
            status &= removePartitionRelation(partitionId);
        }
        return status;
    }

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

    @Override
    public Collection<String> getSecondIds(String serverId, int storageRegionId) {
        return this.secondIds.getSecondIds(serverId,storageRegionId);
    }

    @Override
    public String getSecondId(String partitionId, int storageRegionId) {
        return this.secondIds.getSecondId(partitionId,storageRegionId);
    }

    @Override
    public String getFirstId(String secondId, int storageRegionId) {
        return this.secondIds.getFirstId(secondId,storageRegionId);
    }
}
