package com.bonree.brfs.rebalance.task;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.apache.curator.shaded.com.google.common.collect.Maps;
import org.apache.curator.utils.ZKPaths;

import com.bonree.brfs.common.utils.JsonUtils;
import com.bonree.brfs.common.utils.JsonUtils.JsonException;
import com.bonree.brfs.common.zookeeper.curator.CuratorClient;
import com.google.common.collect.Lists;

public class ReadMetaData {

    public static void main(String[] args) throws JsonException {
        String path = "/brfs/test1/rebalance/changes/0";
        Map<String,List<ChangeSummary>> csMap = Maps.newHashMap();
        CuratorClient curatorClient=CuratorClient.getClientInstance("192.168.107.13");
        List<String> childNodeList=curatorClient.getChildren(path);
        Collections.sort(childNodeList);
        for(String node:childNodeList) {
            String nodePath = ZKPaths.makePath(path, node);
            ChangeSummary cs = JsonUtils.toObject(curatorClient.getData(nodePath), ChangeSummary.class);
            if(csMap.get(cs.getChangeServer()) == null) {
                List<ChangeSummary> csList = Lists.newArrayList();
                csList.add(cs);
                csMap.put(cs.getChangeServer(), csList);
            }else {
                csMap.get(cs.getChangeServer()).add(cs);
            }
            
        }
        for(Entry<String, List<ChangeSummary>> entry : csMap.entrySet()) {
            System.out.println(entry.getKey() + ":" + entry.getValue());
            System.out.println(entry.getKey() + ":" + entry.getValue().stream().map(x -> x.getChangeType()).collect(Collectors.toList()));
        }
        
        curatorClient.close();
    }
}
