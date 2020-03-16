package com.bonree.brfs.common.rebalance.route.v2;

import com.bonree.brfs.common.rebalance.TaskVersion;
import com.bonree.brfs.common.rebalance.route.LocateRouteServerInterface;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.*;

/*******************************************************************************
 * 版权信息：博睿宏远科技发展有限公司
 * Copyright: Copyright (c) 2007博睿宏远科技发展有限公司,Inc.All Rights Reserved.
 * 
 * @date 2020年3月11日 下午14:31:00
 * @Author: <a href=mailto:zhucg@bonree.com>朱成岗</a>
 * @Description: 2级serverID的迁移记录
 ******************************************************************************/
public class NormalRouteV2 implements LocateRouteServerInterface {
    private static final TaskVersion CURRENT_VERSION = TaskVersion.V2;
    @JsonProperty("changeID")
    private String changeID;
    
    @JsonProperty("storageIndex")
    private int storageIndex;
    
    @JsonProperty("secondID")
    private String secondID;

    @Deprecated // 适配低版本路由规则
    @JsonProperty("newSecondIDs")
    private List<String> newSecondIDs;

    @JsonProperty("newSecondIDRatios")
    private List<Pair<String,Integer>> newSecondIDRatios;
    @JsonProperty("version")
    private TaskVersion version;

    @SuppressWarnings("unused")
    private NormalRouteV2() {
    }

    public NormalRouteV2(String changeID, int storageIndex, String secondID, List<String> newSecondIDs, List<Pair<String, Integer>> newSecondIDRatios, TaskVersion version) throws IllegalArgumentException {
        this.changeID = changeID;
        this.storageIndex = storageIndex;
        this.secondID = secondID;
        this.newSecondIDs = null;
        this.newSecondIDRatios = newSecondIDRatios;
        // 版本号判断
        switch(version){
            // 版本1的转换为版本2
            // todo 采用原来的解析方式不好么？
            case V1:
                this.newSecondIDRatios = convertedVersion(newSecondIDs);
                this.version = version;
                break;
            case V2:
                this.newSecondIDRatios = newSecondIDRatios;
                this.version = CURRENT_VERSION;
                break;
            default:
                throw new IllegalArgumentException("Invalid version or higher than 2.0 !! version:["+version+"],changeID:["+changeID+"],storageIndex:["+storageIndex+"]");
        }
        if(this.newSecondIDRatios == null ||this.newSecondIDRatios.isEmpty()){
            throw new IllegalArgumentException("[version 2.0] accept invalid routing rule!! {}");
        }
    }

    public NormalRouteV2(String changeID, int storageIndex, String secondID, List<Pair<String, Integer>> newSecondIDRatios, TaskVersion version) {
        this(changeID,storageIndex,secondID,null,newSecondIDRatios,version);
    }

    private List<Pair<String,Integer>>  convertedVersion(List<String> list){
        if(list == null || list.isEmpty()){
            return null;
        }
        List<Pair<String,Integer>> v2List = new ArrayList<>();
        for(String ele : list){
            v2List.add(new Pair<>(ele,1));
        }
        // 兼容版本1逻辑，版本一在选择时，进行排序了。
        Collections.sort(v2List, new Comparator<Pair<String, Integer>>() {
            @Override
            public int compare(Pair<String, Integer> o1, Pair<String, Integer> o2) {
                return o1.getKey().compareTo(o2.getKey());
            }
        });
        return v2List;
    }

    public String getChangeID() {
        return changeID;
    }

    public void setChangeID(String changeID) {
        this.changeID = changeID;
    }

    public int getStorageIndex() {
        return storageIndex;
    }

    public void setStorageIndex(int storageIndex) {
        this.storageIndex = storageIndex;
    }

    public String getSecondID() {
        return secondID;
    }

    public void setSecondID(String secondID) {
        this.secondID = secondID;
    }

    public List<String> getNewSecondIDs() {
        return newSecondIDs;
    }

    public void setNewSecondIDs(List<String> newSecondIDs) {
        this.newSecondIDs = newSecondIDs;
    }

    public List<Pair<String, Integer>> getNewSecondIDRatios() {
        return newSecondIDRatios;
    }

    public void setNewSecondIDRatios(List<Pair<String, Integer>> newSecondIDRatios) {
        this.newSecondIDRatios = newSecondIDRatios;
    }

    public TaskVersion getVersion() {
        return version;
    }

    public void setVersion(TaskVersion version) {
        this.version = version;
    }

    @Override
    public String locateNormalServer(String fileUUID, Collection<String> services) {
        List<Pair<String,Integer>> selectArray = new ArrayList<>();
        // 1.找出不参与选择的服务
        Pair<Integer, Collection<String>> filterPair = collectUnusdServer(services);

        int total = filterPair.getKey();
        Collection<String> filterServers = filterPair.getValue();

        // 2.获取文件的权值
        int indexValue = hashFileName(fileUUID,total);
        // 3 根据权值查找
        int index = searchIndex(filterServers,indexValue);
        // 4.检查合法性，若不合法，则检索失败，此处路由规则存在问题，会导致严重的数据丢失问题。
        if(index<0||index>this.newSecondIDRatios.size()){
            // todo 定义抛出异常的格式
            throw new IllegalFormatWidthException(1);
        }
        return this.newSecondIDRatios.get(index).getKey();
    }

    /**
     * 路由规则V2版本检索二级serverId逻辑
     * @param filterServers
     * @param indexValue
     * @return
     */
    private int searchIndex(Collection<String> filterServers,int indexValue){
        if(indexValue == 0){
            return 0;
        }
        int sum = 0;
        int lastVaild = -1;
        Pair<String,Integer> pair = null;
        for(int index=0;index<this.newSecondIDRatios.size();index++){
            pair = this.newSecondIDRatios.get(index);
            if(filterServers.contains(pair.getKey())){
                continue;
            }
            sum +=pair.getValue();
            if(indexValue>sum){
                lastVaild = index;
            }else{
                break;
            }
        }
        return lastVaild;
    }

    /**
     * 将不可用的服务过滤出来
     * @param services
     * @return
     */
    private Pair<Integer,Collection<String>> collectUnusdServer(Collection<String> services){
        Collection<String> selectArray = new HashSet<>();
        // 1.剔除不参与选择的服务
        String serverId = null;
        int total = 0;
        for(Pair<String,Integer> pair :this.newSecondIDRatios){
            serverId = pair.getKey();
            if(services.contains(serverId)){
                continue;
            }
            total+=pair.getValue();
            selectArray.add(serverId);
        }
        return new Pair<Integer, Collection<String>>(total,selectArray);
    }

    /**
     * 文件名生成数值 V1版本使用的。V2兼容
     * @param fileName
     * @param size
     * @return
     */
    private int hashFileName(String fileName, int size) {
        int nameSum = sumName(fileName);
        int matchSm = nameSum % size;
        return matchSm;
    }

    private int sumName(String name) {
        int sum = 0;
        for (int i = 0; i < name.length(); i++) {
            sum = sum + name.charAt(i);
        }
        return sum;
    }
}
