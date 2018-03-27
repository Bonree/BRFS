package com.bonree.brfs.rebalance.task;

import java.util.Date;
import java.util.Map;


import com.bonree.brfs.server.model.StorageName;
import com.google.common.collect.Maps;

public class TaskDiscoverTest {

    public static Map<Integer, StorageName> storageMap = Maps.newHashMap();
    private static int TTL = 30 * 24 * 3600;
    private static long triggerRecoverTime = new Date().getTime() + 3600;
    static {
        StorageName s1 = new StorageName();
        s1.setIndex(1);
        s1.setStorageName("sdk");
        s1.setReplications(2);
        s1.setDescription("sdk");
        s1.setTtl(TTL);
        s1.setTriggerRecoverTime(triggerRecoverTime);

        StorageName s2 = new StorageName();
        s2.setIndex(2);
        s2.setStorageName("v4");
        s2.setReplications(2);
        s2.setDescription("v4");
        s2.setTtl(TTL);
        s2.setTriggerRecoverTime(triggerRecoverTime);

        StorageName s3 = new StorageName();
        s3.setIndex(3);
        s3.setStorageName("test");
        s3.setReplications(2);
        s3.setDescription("test");
        s3.setTtl(TTL);
        s3.setTriggerRecoverTime(triggerRecoverTime);

        storageMap.put(1, s1);
        storageMap.put(2, s2);
        storageMap.put(3, s3);
    }

    public static void main(String[] args) throws InterruptedException {
        
        
    }

}
