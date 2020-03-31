package com.bonree.brfs.partition;

import com.bonree.brfs.common.utils.JsonUtils;
import com.bonree.brfs.partition.model.PartitionInfo;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.utils.ZKPaths;
import org.apache.zookeeper.CreateMode;

import java.util.ArrayList;
import java.util.List;

/*******************************************************************************
 * 版权信息： 北京博睿宏远数据科技股份有限公司
 * Copyright (c) 2007-2020 北京博睿宏远数据科技股份有限公司，Inc. All Rights Reserved.
 * @date 2020年03月24日 15:33:54
 * @author: <a href=mailto:zhucg@bonree.com>朱成岗</a>
 * @description:磁盘节点注册节点 zookeeper的连接时间要短 否则会造成进程不在，但还可以访问的问题
 ******************************************************************************/

public class PartitionInfoRegister {
    private CuratorFramework framework;
    private String zkBasePath = "/discovery/diskgroup";

    public PartitionInfoRegister(CuratorFramework framework, String zkBasePath) {
        this.framework = framework;
        this.zkBasePath = zkBasePath;
    }

    public void registerPartitionInfo(PartitionInfo partitionInfo) throws Exception{
        byte[] data = JsonUtils.toJsonBytes(partitionInfo);
        String zkPath = ZKPaths.makePath(this.zkBasePath,partitionInfo.getPartitionGroup(),partitionInfo.getPartitionId());
    }
    public void unregisterPartitionInfo(PartitionInfo partitionInfo) throws Exception{
        String zkPath = ZKPaths.makePath(this.zkBasePath,partitionInfo.getPartitionGroup(),partitionInfo.getPartitionId());
        if(framework.checkExists().forPath(zkPath) !=null){
            framework.delete().forPath(zkPath);
        }
    }
    public  void unregisterPartitionInfo(String partitionGroup,String partitionId)throws Exception{
        String zkPath = ZKPaths.makePath(this.zkBasePath,partitionGroup,partitionId);
        if(framework.checkExists().forPath(zkPath) !=null){
            framework.delete().forPath(zkPath);
        }
    }
    public  PartitionInfo getPartitionInfo(String partitionGroup,String partitionId)throws Exception{
        String zkPath = ZKPaths.makePath(this.zkBasePath,partitionGroup,partitionId);
        if(framework.checkExists().forPath(zkPath) ==null){
            return null;
        }
        byte[] data = framework.getData().forPath(zkPath);
        if(data == null ||data.length==0){
            return null;
        }
        return JsonUtils.toObjectQuietly(data,PartitionInfo.class);
    }
    public List<PartitionInfo> listPartitionInfos(String partitionGroup)throws Exception{
        String parentPath = ZKPaths.makePath(this.zkBasePath,partitionGroup);
        if(framework.checkExists().forPath(parentPath) ==null){
            return null;
        }
        List<String> childs = framework.getChildren().forPath(parentPath);
        if(childs == null || childs.isEmpty()){
            return null;
        }
        List<PartitionInfo> array = new ArrayList<>();
        PartitionInfo partitionInfo = null;
        for(String child : childs){
            partitionInfo = getPartitionInfo(partitionGroup,child);
            if(partitionInfo==null){
                continue;
            }
            array.add(partitionInfo);
        }
        return array;
    }

}
