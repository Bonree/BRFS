/*******************************************************************************
 * 版权信息： 北京博睿宏远数据科技股份有限公司
 * Copyright (c) 2007-2020 北京博睿宏远数据科技股份有限公司，Inc. All Rights Reserved.
 * @date 2020年03月19日 16:49:14
 * @author: <a href=mailto:zhucg@bonree.com>朱成岗</a>
 * @description: 路由解析器，
 ******************************************************************************/

package com.bonree.brfs.rebalance.route;

import com.bonree.brfs.common.rebalance.Constants;
import com.bonree.brfs.common.rebalance.route.NormalRouteInterface;
import com.bonree.brfs.common.rebalance.route.VirtualRoute;
import com.bonree.brfs.common.utils.Pair;
import com.bonree.brfs.common.zookeeper.curator.CuratorClient;
import com.bonree.brfs.rebalance.route.factory.SingleRouteFactory;
import org.apache.commons.lang3.StringUtils;

import java.util.*;

public class RouteParser {
    private int storageRegionID;
    private Map<String, NormalRouteInterface> normalRouteTree = new HashMap<>();
    private Map<String, VirtualRoute> virtualRouteRelationship = new HashMap<>();
    private CuratorClient curatorClient;
    private String normalRoutePath;
    private String virtualRoutePath;


    public RouteParser(int storageRegionID, CuratorClient curatorClient, String routePath)throws Exception{
        this.storageRegionID = storageRegionID;
        this.curatorClient = curatorClient;
        this.normalRoutePath = routePath + Constants.SEPARATOR + Constants.VIRTUAL_ROUTE + Constants.SEPARATOR + storageRegionID;
        this.virtualRoutePath = routePath+ Constants.SEPARATOR + Constants.NORMAL_ROUTE + Constants.SEPARATOR+storageRegionID;
        load();
    }

    /**
     * 从zookeeper加载路由规则
     */
    public void load()throws Exception{
        if (curatorClient.checkExists(virtualRoutePath)) {
            List<String> virtualNodes = curatorClient.getChildren(virtualRoutePath);
            if (virtualNodes != null && !virtualNodes.isEmpty()) {
                for (String virtualNode : virtualNodes) {
                    String dataPath = virtualRoutePath + Constants.SEPARATOR + virtualNode;
                    byte[] data = curatorClient.getData(dataPath);
                    VirtualRoute virtual = SingleRouteFactory.createVirtualRoute(data);
                    virtualRouteRelationship.put(virtual.getVirtualID(), virtual);
                }
            }
        }
        if (curatorClient.checkExists(normalRoutePath)) {
            List<String> normalNodes = curatorClient.getChildren(normalRoutePath);
            if (normalNodes != null && !normalNodes.isEmpty()) {
                for (String normalNode : normalNodes) {
                    String dataPath = normalRoutePath + Constants.SEPARATOR + normalNode;
                    byte[] data = curatorClient.getData(dataPath);
                    NormalRouteInterface normal = SingleRouteFactory.createRoute(data);
                    normalRouteTree.put(normal.getBaseSecondId(), normal);
                }
            }
        }
    }

    /**
     * 返回fileBocker 块可用的Ids，注意其与旧版本有区别，旧版本包含uuid，本方法不包含uuid，只包含二级serverid
     * @param fileBocker
     * @return serverids
     */
    public String[] searchVaildIds(String fileBocker){
        // 1.分解文件块的名称
        Pair<String,List<String>> pair = analyzingFileName(fileBocker);
        List<String> secondIds = pair.getSecond();
        String uuid = pair.getFirst();
        // 2.判断文件块是否合法
        if(secondIds == null || secondIds.isEmpty() || StringUtils.isEmpty(uuid)||StringUtils.isBlank(uuid)){
            throw new IllegalStateException("fileBocker is invaild !! content:"+fileBocker);
        }
        String source = null;
        String dent =null;
        for(int index = 0; index<secondIds.size();index++){
            source = secondIds.get(index);
            dent = search(uuid,source,secondIds);
            if(!dent.equals(source)){
                secondIds.set(index,dent);
            }
        }
        return secondIds.toArray(new String[0]);
    }

    /**
     * 单个服务检索
     * @param uuid
     * @param secondId
     * @param excludeSecondIds
     * @return
     */
    private String search(String uuid, String secondId, Collection<String> excludeSecondIds){
        String tmpSecondId = secondId;
        List<String> excludes = new ArrayList<>();
        // 1.判断secondId的类型，若为虚拟serverid 并且未迁移 则返回null
        if (secondId.charAt(0) == Constants.VIRTUAL_ID&&virtualRouteRelationship.get(secondId) == null) {
            return secondId;
            //2. 判断secondId的类型，若为虚拟serverid 并发生迁移则进行转换。
        }else if(secondId.charAt(0) == Constants.VIRTUAL_ID){
            tmpSecondId = virtualRouteRelationship.get(secondId).getNewSecondID();
            excludes.add(tmpSecondId);
        }
        excludes.addAll(excludeSecondIds);

        // 3. 若二级serverId 为空则返回null值
        if(StringUtils.isEmpty(tmpSecondId)){
            return secondId;
        }
        // 4. 检索正常的路由规则
        return searchNormalRouteTree(uuid,secondId,excludes);
    }

    /**
     * 检索正常的路由规则
     * @param uuid
     * @param secondId
     * @param excludes
     * @return
     */
    private String searchNormalRouteTree(String uuid,String secondId,Collection<String> excludes){
        if(this.normalRouteTree.get(secondId) == null){
            return secondId;
        }else{
            String tmpSI = this.normalRouteTree.get(secondId).locateNormalServer(uuid,excludes);
            return searchNormalRouteTree(uuid,tmpSI,excludes);
        }
    }

    /**
     * 解析文件块名称
     * @param fileBocker 文件块名称
     * @return Pair\<String,List\<String>> key: 文件的uuid，value 二级serverId集合，按照其顺序排列
     */
    private Pair<String, List<String>> analyzingFileName(String fileBocker){
        String[] splitStr = fileBocker.split("_");
        String fileUUID = splitStr[0];
        List<String> fileServerIDs = new ArrayList<>(splitStr.length - 1);
        for (int i = 1; i < splitStr.length; i++) {
            fileServerIDs.add(splitStr[i]);
        }
        return new Pair<>(fileUUID,fileServerIDs);
    }
}
