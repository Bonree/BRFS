package com.bonree.brfs.rebalance.route.impl;

import com.bonree.brfs.common.rebalance.Constants;
import com.bonree.brfs.common.rebalance.route.NormalRouteInterface;
import com.bonree.brfs.common.rebalance.route.VirtualRoute;
import com.bonree.brfs.common.utils.Pair;
import com.bonree.brfs.common.zookeeper.curator.CuratorClient;
import com.bonree.brfs.rebalance.route.BlockAnalyzer;
import com.bonree.brfs.rebalance.route.RouteLoader;
import com.bonree.brfs.rebalance.route.factory.SingleRouteFactory;
import org.apache.commons.lang3.StringUtils;
import org.apache.curator.framework.CuratorFramework;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.stream.Stream;

/*******************************************************************************
 * 版权信息： 北京博睿宏远数据科技股份有限公司
 * Copyright (c) 2007-2020 北京博睿宏远数据科技股份有限公司，Inc. All Rights Reserved.
 * @date 2020年03月30日 17:18:50
 * @author: <a href=mailto:zhucg@bonree.com>朱成岗</a>
 * @description:
 ******************************************************************************/

public class RouteParser implements BlockAnalyzer {
    private static final Logger LOG = LoggerFactory.getLogger(RouteLoader.class);
    private int storageRegionID;
    private Map<String, NormalRouteInterface> normalRouteTree = new HashMap<>();
    private Map<String, VirtualRoute> virtualRouteRelationship = new HashMap<>();
    private RouteLoader loader = null;


    public RouteParser(int storageRegionID,RouteLoader loader){
        this.storageRegionID = storageRegionID;
        this.loader = loader;
        update();
    }

    /**
     * 返回fileBocker 块可用的Ids，注意其与旧版本有区别
     * @param fileBocker
     * @return serverids
     */
    @Override
    public String[] searchVaildIds(String fileBocker){
        // 1.分解文件块的名称
        Pair<String,List<String>> pair = analyzingFileName(fileBocker);
        List<String> secondIds = pair.getSecond();
        String uuid = pair.getFirst();
        // 2.判断文件块是否合法
        if(secondIds == null || secondIds.isEmpty() || StringUtils.isEmpty(uuid)||StringUtils.isBlank(uuid)){
            throw new IllegalStateException("fileBocker is invaild !! content:"+fileBocker);
        }
        int fileCode =sumName(uuid);
        String source = null;
        String dent =null;
        for(int index = 0; index<secondIds.size();index++){
            source = secondIds.get(index);
            dent = search(fileCode,source,secondIds);
            if(!dent.equals(source)){
                secondIds.set(index,dent);
            }
        }
        return secondIds.toArray(new String[0]);
    }

    @Override
    public void update() {
        try {
            Collection<VirtualRoute> virtualRoutes = this.loader.loadVirtualRoutes(this.storageRegionID);
            Collection<NormalRouteInterface>normalRoutes = this.loader.loadNormalRoutes(this.storageRegionID);
            if(virtualRoutes != null && !virtualRoutes.isEmpty()){

            }
            if(normalRoutes !=null && !normalRoutes.isEmpty()){
                for(NormalRouteInterface normal : normalRoutes){
                    this.normalRouteTree.put(normal.getBaseSecondId(),normal);
                }
            }
            if(virtualRoutes !=null && !virtualRoutes.isEmpty()){
                for(VirtualRoute virtualRoute : virtualRoutes){
                    this.virtualRouteRelationship.put(virtualRoute.getVirtualID(),virtualRoute);
                }
            }
        } catch (Exception e) {
            LOG.error("load data happen error !!");
        }
    }

    /**
     * 单个服务检索
     * 性能可以进一步提升，uuid事先计算出数值
     * @param fileCode
     * @param secondId
     * @param excludeSecondIds
     * @return
     */
    private String search(int fileCode, String secondId, Collection<String> excludeSecondIds){
        String tmpSecondId = secondId;
        List<String> excludes = new ArrayList<>();
        // 1.判断secondId的类型，若为虚拟serverid 并且未迁移 则返回null
        if (secondId.charAt(0) == Constants.VIRTUAL_ID&&virtualRouteRelationship.get(secondId) == null) {
            return secondId;
            //2. 判断secondId的类型，若为虚拟serverid 并发生迁移则进行转换。
        }else if(secondId.charAt(0) == Constants.VIRTUAL_ID){
            tmpSecondId = virtualRouteRelationship.get(secondId).getNewSecondID();
        }
        excludes.addAll(excludeSecondIds);

        // 3. 若二级serverId 为空则返回null值
        if(StringUtils.isEmpty(tmpSecondId)){
            return secondId;
        }
        // 4. 检索正常的路由规则
        return searchNormalRouteTree(fileCode,tmpSecondId,excludes);
    }

    /**
     * 检索正常的路由规则
     * @param fileCode
     * @param secondId
     * @param excludes
     * @return
     */
    private String searchNormalRouteTree(int fileCode,String secondId,Collection<String> excludes){
        if(this.normalRouteTree.get(secondId) == null){
            return secondId;
        }else{
            String tmpSI = this.normalRouteTree.get(secondId).locateNormalServer(fileCode,excludes);
            return searchNormalRouteTree(fileCode,tmpSI,excludes);
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

    /**
     * 根据文件名生成code
     * @param name
     * @return
     */
    protected int sumName(String name) {
        int sum = 0;
        for (int i = 0; i < name.length(); i++) {
            sum = sum + name.charAt(i);
        }
        return sum;
    }
}
