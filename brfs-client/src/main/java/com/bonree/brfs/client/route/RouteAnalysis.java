package com.bonree.brfs.client.route;

import com.bonree.brfs.common.rebalance.Constants;

import java.util.*;
import java.util.stream.Collectors;

public class RouteAnalysis {
    private Map<String,NormalRouterNode> normalRouterNodeMap = new HashMap<>();
    private Map<String,VirtualRouterNode> virtualRouterNodeMap = new HashMap<>();
    public List<String> analysisRoutes(String uuid,List<String> services){
        int fileCode = sumName(uuid);
        List<String> dentSecondIds = new ArrayList<>();
        for(String secondId : services){
           String dentSecond = analysisRoute(fileCode,secondId,services);
           if(dentSecond == null || dentSecond.trim().isEmpty()){
               dentSecondIds.add(secondId);
           }else {
               dentSecondIds.add(dentSecond);
           }
        }
        return dentSecondIds;
    }

    /**
     * 解析单个uuid
     * @param fileNode
     * @param secondId
     * @param services
     * @return
     */
    public String analysisRoute(String fileNode, String secondId, List<String> services){
        int filecode = sumName(fileNode);
        return analysisRoute(filecode,secondId,services);
    }

    /**
     * 解析路由规则
     * @param code
     * @param secondId
     * @param service
     * @return
     */
    private String analysisRoute(int code, String secondId, List<String> service){
        String dentSecond = analysisVirtual(secondId);
        if(dentSecond == null || dentSecond.trim().isEmpty()){
            return secondId;
        }
        while(normalRouterNodeMap.get(dentSecond) !=null){
            NormalRouterNode normalRouterNode = normalRouterNodeMap.get(dentSecond);
            dentSecond = analysisNormal(code,dentSecond,service,normalRouterNode);
        }
        return dentSecond;
    }
    private String analysisNormal(int code, String secondId, List<String> services, NormalRouterNode routerNode){
        Map<String,Integer> map = routerNode.getNewSecondIDs();
        if(map == null || map.isEmpty()){
            return secondId;
        }
        List<String> selector = filterService(map.keySet(),services);
        if(selector == null|| selector.isEmpty()){
            return secondId;
        }
        int weightValue = calcWeight(selector,map);
        if(weightValue<=0){
            return secondId;
        }
        int WeightIndex = hashFileName(code,weightValue);
        int index = searchIndex(selector,map,WeightIndex);
        return selector.get(index);
    }

    /**
     * 过滤不参与的服务
     * @param newSecondIDs
     * @param services
     * @return
     */
    private List<String> filterService(Collection<String> newSecondIDs, Collection<String> services){
        List<String> selectors = new ArrayList<>();
        // 1.过滤掉已经使用的service
        if(services!=null&& !services.isEmpty()){
            selectors = newSecondIDs.stream().filter(x->{return !services.contains(x);}).collect(Collectors.toList());
        }else {
            selectors.addAll(newSecondIDs);
        }
        // 2.判断集合是否为空，为空，则解析失败。
        if(selectors.isEmpty()){
            throw new IllegalArgumentException("errror");
        }
        // 3.对select 服务进行排序。
        Collections.sort(selectors, new Comparator<String>() {
            @Override
            public int compare(String o1, String o2) {
                return o1.compareTo(o2);
            }
        });
        return selectors;
    }
    /**
     * 计算权值
     * @param services
     * @return
     */
    private int calcWeight(Collection<String> services,Map<String,Integer> newSecondIDs) {
        // 1.若services的为空则该权值计算无效返回-1
        if(services == null || services.isEmpty()){
            return -1;
        }
        // 2. 累计权值
        int weight = 0;
        int tmp = -1;
        for (String service : services) {
            if (newSecondIDs.get(service) != null) {
                tmp = newSecondIDs.get(service);
                weight+=tmp;
            }
        }
        // 3.若tmp为-1 则表明未匹配上service，则其权值计算无效，范围-1
        return tmp == -1 ? -1 : weight;
    }
    /**
     * 文件名生成数值 V1版本使用的。V2兼容
     *
     * @param fileCode
     * @param size
     * @return
     */
    private int hashFileName(int fileCode, int size) {
        int matchSm = fileCode % size;
        return matchSm;
    }
    /**
     * 路由规则V2版本检索二级serverId逻辑
     *
     * @param chosenServices
     * @param weightValue
     * @return
     */
    private int searchIndex(List<String> chosenServices, Map<String,Integer> newSecondIDs,int weightValue) {
        if (weightValue == 0) {
            return 0;
        }
        int sum = 0;
        int lastVaild = -1;
        String server = null;
        for (int index = 0; index < chosenServices.size(); index++) {
            server = chosenServices.get(index);
            if (newSecondIDs.get(server)==null) {
                continue;
            }
            sum += newSecondIDs.get(server);
            lastVaild = index;
            if (weightValue <= sum) {
                break;
            }
        }
        return lastVaild;
    }

    /**
     * 解析虚拟serverid
     * @param secondId
     * @return
     */
    private String analysisVirtual(String secondId){
        // 若不为虚拟serverid则返回原始值
        if(secondId.charAt(0) != Constants.VIRTUAL_ID){
            return secondId;
        }
        // 若为虚拟serverid 并且未迁移 则返回null
        if (virtualRouterNodeMap.get(secondId) == null) {
            return null;
        }
        VirtualRouterNode virtual = virtualRouterNodeMap.get(secondId);
        return virtual.getNewSecondId();
    }

    /**
     * 根据文件名生成code
     * @param name
     * @return
     */
    private int sumName(String name) {
        int sum = 0;
        for (int i = 0; i < name.length(); i++) {
            sum = sum + name.charAt(i);
        }
        return sum;
    }

    public Map<String, NormalRouterNode> getNormalRouterNodeMap() {
        return normalRouterNodeMap;
    }

    public void setNormalRouterNodeMap(Map<String, NormalRouterNode> normalRouterNodeMap) {
        this.normalRouterNodeMap = normalRouterNodeMap;
    }

    public Map<String, VirtualRouterNode> getVirtualRouterNodeMap() {
        return virtualRouterNodeMap;
    }

    public void setVirtualRouterNodeMap(Map<String, VirtualRouterNode> virtualRouterNodeMap) {
        this.virtualRouterNodeMap = virtualRouterNodeMap;
    }
}
