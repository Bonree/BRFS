package com.bonree.brfs.duplication.filenode.duplicates.impl;

import com.bonree.brfs.common.service.Service;
import com.bonree.brfs.common.service.ServiceManager;
import com.bonree.brfs.duplication.datastream.connection.DiskNodeConnection;
import com.bonree.brfs.duplication.datastream.connection.DiskNodeConnectionPool;
import com.bonree.brfs.duplication.filenode.duplicates.DuplicateNode;
import com.bonree.brfs.duplication.filenode.duplicates.DuplicateNodeSelector;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.slf4j.Logger;

public class MinimalDuplicateNodeSelector implements DuplicateNodeSelector {
    private ServiceManager serviceManager;
    private DiskNodeConnectionPool connectionPool;
    private Random rand = new Random();
    private Logger log = null;
    private String dataGroup = null;

    public MinimalDuplicateNodeSelector(ServiceManager serviceManager, DiskNodeConnectionPool connectionPool, Logger log,
                                        String dataGroup) {
        this.serviceManager = serviceManager;
        this.connectionPool = connectionPool;
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
        while (!serviceList.isEmpty() && nodes.size() < nums) {
            Service service = serviceList.remove(rand.nextInt(serviceList.size()));

            DuplicateNode node = new DuplicateNode(service.getServiceGroup(), service.getServiceId());
            DiskNodeConnection conn = connectionPool.getConnection(node.getGroup(), node.getId());
            if (conn == null || conn.getClient() == null || !conn.getClient().ping()) {
                continue;
            }

            nodes.add(node);
        }
        DuplicateNode[] result = new DuplicateNode[nodes.size()];
        long end = System.currentTimeMillis();
        log.info("select time {}ms,serverId {}", (end - start), nodes);
        return nodes.toArray(result);
    }

}
