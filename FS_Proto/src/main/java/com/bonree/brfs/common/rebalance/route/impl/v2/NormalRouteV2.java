package com.bonree.brfs.common.rebalance.route.impl.v2;

import com.bonree.brfs.common.rebalance.TaskVersion;
import com.bonree.brfs.common.rebalance.route.impl.SuperNormalRoute;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Collection;
import java.util.List;
import java.util.Map;


/*******************************************************************************
 * 版权信息：博睿宏远科技发展有限公司
 * Copyright: Copyright (c) 2007博睿宏远科技发展有限公司,Inc.All Rights Reserved.
 *
 * @date 2020年3月11日 下午14:31:00
 * @Author: <a href=mailto:zhucg@bonree.com>朱成岗</a>
 * @Description: 2级serverID的迁移记录
 ******************************************************************************/
public class NormalRouteV2 extends SuperNormalRoute {
    private static final TaskVersion CURRENT_VERSION = TaskVersion.V2;
    private Map<String, Integer> newSecondIDs;

    public NormalRouteV2(@JsonProperty("changeID") String changeID,
                         @JsonProperty("storageIndex") int storageIndex,
                         @JsonProperty("secondID") String secondID,
                         @JsonProperty("newSecondIDs") Map<String, Integer> newSecondIDs) {
        super(changeID, storageIndex, secondID, CURRENT_VERSION);
        this.newSecondIDs = newSecondIDs;
    }

    public Map<String, Integer> getNewSecondIDs() {
        return newSecondIDs;
    }

    public void setNewSecondIDs(Map<String, Integer> newSecondIDs) {
        this.newSecondIDs = newSecondIDs;
    }

    @Override
    public String locateNormalServer(int fileUUIDCode, Collection<String> services) {

        // 1.找出参与选择的服务
        List<String> chosenService = filterService(services);
        // 2. 获取计算的权重
        int weight = calcWeight(chosenService);
        // 3.根据权重计算权值
        int weightValue = hashFileName(fileUUIDCode, weight);
        // 4 根据权值确定数组的序号
        int index = searchIndex(chosenService, weightValue);
        // 5.返回serverid
        return chosenService.get(index);
    }

    @Override
    public Map<String, Integer> getRoutes() {
        return this.newSecondIDs;
    }

    /**
     * 路由规则V2版本检索二级serverId逻辑
     *
     * @param chosenServices
     * @param weightValue
     * @return
     */
    private int searchIndex(List<String> chosenServices, int weightValue) {
        if (weightValue == 0) {
            return 0;
        }
        int sum = 0;
        int lastVaild = -1;
        String server = null;
        for (int index = 0; index < chosenServices.size(); index++) {
            server = chosenServices.get(index);
            if (this.newSecondIDs.get(server) == null) {
                continue;
            }
            sum += this.newSecondIDs.get(server);
            lastVaild = index;
            if (weightValue <= sum) {
                break;
            }
        }
        return lastVaild;
    }

    /**
     * 过滤
     *
     * @param services
     * @return
     */
    private List<String> filterService(Collection<String> services) {
        return filterService(this.newSecondIDs.keySet(), services);
    }

    /**
     * 计算权值
     *
     * @param services
     * @return
     */
    private int calcWeight(Collection<String> services) {
        // 1.若services的为空则该权值计算无效返回-1
        if (services == null || services.isEmpty()) {
            return -1;
        }
        // 2. 累计权值
        int weight = 0;
        int tmp = -1;
        for (String service : services) {
            if (this.newSecondIDs.get(service) != null) {
                tmp = this.newSecondIDs.get(service);
                weight += tmp;
            }
        }
        // 3.若tmp为-1 则表明未匹配上service，则其权值计算无效，范围-1
        return tmp == -1 ? -1 : weight;
    }

    @Override
    public String toString() {
        return "NormalRouteV2{"
            + "newSecondIDs=" + newSecondIDs
            + ", changeID='" + changeID + '\''
            + ", storageIndex=" + storageIndex
            + ", secondID='" + secondID + '\''
            + ", version=" + version
            + '}';
    }
}
