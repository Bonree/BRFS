package com.bonree.brfs.rebalance.route.impl;

import com.bonree.brfs.common.rebalance.route.NormalRouteInterface;
import com.bonree.brfs.common.rebalance.route.VirtualRoute;
import com.bonree.brfs.common.utils.Pair;
import com.bonree.brfs.rebalance.route.BlockAnalyzer;
import com.bonree.brfs.rebalance.route.RouteLoader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    public RouteParser(int storageRegionID, RouteLoader loader) {
        this(storageRegionID, loader, true);
    }

    public RouteParser(int storageRegionID, RouteLoader loader, boolean update) {
        this.storageRegionID = storageRegionID;
        this.loader = loader;
        if (update) {
            update();
        }
    }

    /**
     * 返回fileBocker 块可用的Ids，注意其与旧版本有区别
     *
     * @param fileBocker
     * @return serverids
     */
    public String[] searchVaildIdsbak(String fileBocker) {
        // 1.分解文件块的名称
        Pair<String, List<String>> pair = BlockAnalyzer.analyzingFileName(fileBocker);
        List<String> secondIds = pair.getSecond();
        String uuid = pair.getFirst();
        // 2.判断文件块是否合法
        if (secondIds == null || secondIds.isEmpty() || StringUtils.isEmpty(uuid) || StringUtils.isBlank(uuid)) {
            throw new IllegalStateException("fileBocker is invaild !! content:" + fileBocker);
        }
        // 路由解析时，需要将发生迁移的虚拟serverid转换为等效的二级serverid
        int length = secondIds.size();
        for (int i = 0; i < length; i++) {
            String tmp = secondIds.get(i);
            if (virtualRouteRelationship.get(tmp) != null) {
                tmp = virtualRouteRelationship.get(tmp).getNewSecondID();
                secondIds.set(i, tmp);
            }
        }
        int fileCode = BlockAnalyzer.sumName(uuid);
        String source = null;
        String dent = null;
        for (int index = 0; index < secondIds.size(); index++) {
            source = secondIds.get(index);
            dent = search(fileCode, source, secondIds);
            if (!dent.equals(source)) {
                secondIds.set(index, dent);
                LOG.info("source {} dent {}", source, dent);
            }
        }
        return secondIds.toArray(new String[0]);
    }

    @Override
    public String[] searchVaildIds(String fileBocker) {
        // 1.分解文件块的名称
        Pair<String, List<String>> pair = BlockAnalyzer.analyzingFileName(fileBocker);
        List<String> secondIds = pair.getSecond();
        String uuid = pair.getFirst();
        // 2.判断文件块是否合法
        if (secondIds == null || secondIds.isEmpty() || StringUtils.isEmpty(uuid) || StringUtils.isBlank(uuid)) {
            throw new IllegalStateException("fileBocker is invaild !! content:" + fileBocker);
        }
        // 路由解析时，需要将发生迁移的虚拟serverid转换为等效的二级serverid
        int length = secondIds.size();
        for (int i = 0; i < length; i++) {
            String tmp = secondIds.get(i);
            if (virtualRouteRelationship.get(tmp) != null) {
                tmp = virtualRouteRelationship.get(tmp).getNewSecondID();
                secondIds.set(i, tmp);
            }
        }
        int fileCode = BlockAnalyzer.sumName(uuid);
        return searchNomalRouteTree(fileCode, secondIds).toArray(new String[length]);
    }

    @Override
    public void update() {
        try {
            Collection<VirtualRoute> virtualRoutes = this.loader.loadVirtualRoutes(this.storageRegionID);
            Collection<NormalRouteInterface> normalRoutes = this.loader.loadNormalRoutes(this.storageRegionID);
            if (normalRoutes != null && !normalRoutes.isEmpty()) {
                for (NormalRouteInterface normal : normalRoutes) {
                    this.normalRouteTree.put(normal.getBaseSecondId(), normal);
                }
            }
            if (virtualRoutes != null && !virtualRoutes.isEmpty()) {
                for (VirtualRoute virtualRoute : virtualRoutes) {
                    this.virtualRouteRelationship.put(virtualRoute.getVirtualID(), virtualRoute);
                }
            }
        } catch (Exception e) {
            LOG.error("load data happen error !!");
        }
    }

    @Override
    public void putVirtualRoute(VirtualRoute virtualRoute) {
        if (virtualRoute == null) {
            return;
        }
        this.virtualRouteRelationship.put(virtualRoute.getVirtualID(), virtualRoute);
    }

    @Override
    public void putNormalRoute(NormalRouteInterface routeInterface) {
        if (routeInterface == null) {
            return;
        }
        this.normalRouteTree.put(routeInterface.getBaseSecondId(), routeInterface);
    }

    @Override
    public boolean isRoute(String secondId) {
        if (BlockAnalyzer.isVirtualID(secondId)) {
            return this.virtualRouteRelationship.get(secondId) != null;
        } else {
            return this.normalRouteTree.get(secondId) != null;
        }
    }

    /**
     * 单个服务检索
     * 性能可以进一步提升，uuid事先计算出数值
     *
     * @param fileCode
     * @param secondId
     * @param excludeSecondIds
     * @return
     */
    private String search(int fileCode, String secondId, Collection<String> excludeSecondIds) {
        String tmpSecondId = secondId;
        List<String> excludes = new ArrayList<>();
        // 1.判断secondId的类型，若为虚拟serverid 并且未迁移 则返回null
        if (BlockAnalyzer.isVirtualID(secondId) && virtualRouteRelationship.get(secondId) == null) {
            return secondId;
            //2. 判断secondId的类型，若为虚拟serverid 并发生迁移则进行转换。
        } else if (BlockAnalyzer.isVirtualID(secondId)) {
            tmpSecondId = virtualRouteRelationship.get(secondId).getNewSecondID();
        }
        excludes.addAll(excludeSecondIds);

        // 3. 若二级serverId 为空则返回null值
        if (StringUtils.isEmpty(tmpSecondId)) {
            return secondId;
        }
        // 4. 检索正常的路由规则
        return searchNormalRouteTree(fileCode, tmpSecondId, excludes);
    }

    /**
     * 检索正常的路由规则
     *
     * @param fileCode
     * @param secondId
     * @param excludes
     * @return
     */
    private String searchNormalRouteTree(int fileCode, String secondId, Collection<String> excludes) {
        if (this.normalRouteTree.get(secondId) == null) {
            return secondId;
        } else {
            String tmpSI = this.normalRouteTree.get(secondId).locateNormalServer(fileCode, excludes);
            return searchNormalRouteTree(fileCode, tmpSI, excludes);
        }
    }

    private List<String> searchNomalRouteTree(int fileCode, List<String> secondIds) {
        while (isContain(secondIds)) {
            for (int i = 0; i < secondIds.size(); i++) {
                String source = secondIds.get(i);
                NormalRouteInterface route = this.normalRouteTree.get(source);
                if (route != null) {
                    String dent = route.locateNormalServer(fileCode, secondIds);
                    if (!source.equals(dent)) {
                        secondIds.set(i, dent);
                    }
                }
            }
        }
        return secondIds;
    }

    private boolean isContain(List<String> secondIds) {
        return secondIds.stream().filter(id -> {
            return this.normalRouteTree.get(id) != null;
        }).collect(Collectors.toList()).size() > 0;
    }

    public Map<String, NormalRouteInterface> getNormalRouteTree() {
        return normalRouteTree;
    }

    public void setNormalRouteTree(Map<String, NormalRouteInterface> normalRouteTree) {
        this.normalRouteTree = normalRouteTree;
    }

    public Map<String, VirtualRoute> getVirtualRouteRelationship() {
        return virtualRouteRelationship;
    }

    public void setVirtualRouteRelationship(Map<String, VirtualRoute> virtualRouteRelationship) {
        this.virtualRouteRelationship = virtualRouteRelationship;
    }

    @Override
    public RouteParser clone() {
        RouteParser clone = new RouteParser(this.storageRegionID, this.loader, false);
        clone.setNormalRouteTree(new HashMap<>(this.normalRouteTree));
        clone.setVirtualRouteRelationship(new HashMap<>(this.virtualRouteRelationship));
        return clone;
    }
}
