package com.bonree.brfs.duplication.filenode.duplicates.impl;

import com.bonree.brfs.common.service.Service;
import com.bonree.brfs.common.service.ServiceManager;
import com.bonree.brfs.common.utils.ListUtils;
import com.bonree.brfs.duplication.datastream.file.DuplicateNodeChecker;
import com.bonree.brfs.duplication.filenode.duplicates.DuplicateNode;
import com.bonree.brfs.duplication.filenode.duplicates.DuplicateNodeSelector;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MinimalDuplicateNodeSelector implements DuplicateNodeSelector {
    public static final Logger log = LoggerFactory.getLogger(MinimalDuplicateNodeSelector.class);
    private ServiceManager serviceManager;
    private String dataGroup = null;
    private DuplicateNodeChecker checker;

    public MinimalDuplicateNodeSelector(ServiceManager serviceManager, String dataGroup, DuplicateNodeChecker checker) {
        this.serviceManager = serviceManager;
        this.dataGroup = dataGroup;
        this.checker = checker;
    }

    @Override
    public DuplicateNode[] getDuplicationNodes(int storageId, int nums) {
        List<Service> serviceList = serviceManager.getServiceListByGroup(dataGroup);
        if (serviceList.isEmpty()) {
            return new DuplicateNode[0];
        }
        int size = serviceList.size();
        long start = System.currentTimeMillis();
        List<DuplicateNode> selects = new ArrayList<>();
        if (size >= nums) {
            Iterator<Service> iterator = ListUtils.random(serviceList);
            for (int i = 0; i < nums; i++) {
                Service service = iterator.next();
                DuplicateNode node = new DuplicateNode(service.getServiceGroup(), service.getServiceId(), null);
                selects.add(node);
            }
        } else {
            for (Service service : serviceList) {
                DuplicateNode node = new DuplicateNode(service.getServiceGroup(), service.getServiceId(), null);
                selects.add(node);
            }
        }
        DuplicateNode[] result = new DuplicateNode[selects.size()];
        long end = System.currentTimeMillis();
        log.info("select time {}ms,serverId {}", (end - start), selects);
        return selects.toArray(result);
    }

}
