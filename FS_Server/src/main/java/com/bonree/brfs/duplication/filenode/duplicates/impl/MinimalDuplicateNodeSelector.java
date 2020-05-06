package com.bonree.brfs.duplication.filenode.duplicates.impl;

import com.bonree.brfs.common.service.Service;
import com.bonree.brfs.common.service.ServiceManager;
import com.bonree.brfs.duplication.datastream.connection.DiskNodeConnection;
import com.bonree.brfs.duplication.datastream.connection.DiskNodeConnectionPool;
import com.bonree.brfs.duplication.filenode.duplicates.DuplicateNode;
import com.bonree.brfs.duplication.filenode.duplicates.DuplicateNodeSelector;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.Random;

import org.slf4j.Logger;

public class MinimalDuplicateNodeSelector implements DuplicateNodeSelector {
    private ServiceManager serviceManager;
    private Random rand = new Random();
    private Logger log = null;
    private String dataGroup = null;

    public MinimalDuplicateNodeSelector(
            ServiceManager serviceManager, Logger log, String dataGroup) {
        this.serviceManager = serviceManager;
        this.log = log;
        this.dataGroup = dataGroup;
    }

    @Override
    public DuplicateNode[] getDuplicationNodes(int storageId, int nums) {
        List<Service> serviceList = serviceManager.getServiceListByGroup(dataGroup);
        if (serviceList.isEmpty()) {
            return new DuplicateNode[0];
        }
        long start = System.currentTimeMillis();
        List<DuplicateNode> nodes = new ArrayList<DuplicateNode>();
        BitSet bitSet = new BitSet(serviceList.size());
        while (!serviceList.isEmpty() && nodes.size() < nums) {
            int index = rand.nextInt(serviceList.size());
            if (bitSet.get(index)) {
                continue;
            }
            bitSet.set(index);
            Service service = serviceList.get(index);
            DuplicateNode node = new DuplicateNode(service.getServiceGroup(), service.getServiceId());
            nodes.add(node);
        }
        DuplicateNode[] result = new DuplicateNode[nodes.size()];
        long end = System.currentTimeMillis();
        log.info("select time {}ms,serverId {}", (end - start), nodes);
        return nodes.toArray(result);
    }

}
