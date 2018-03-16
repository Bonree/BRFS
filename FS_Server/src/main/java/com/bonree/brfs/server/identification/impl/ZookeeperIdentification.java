package com.bonree.brfs.server.identification.impl;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bonree.brfs.common.zookeeper.curator.CuratorClient;
import com.bonree.brfs.common.zookeeper.curator.locking.CuratorLocksClient;
import com.bonree.brfs.server.identification.Identification;

public class ZookeeperIdentification implements Identification {

    private static final Logger LOG = LoggerFactory.getLogger(ZookeeperIdentification.class);
    private final String basePath;
    private CuratorClient client;

    private final static String SINGLE_NODE = "/single";

    private final static String MULTI_NODE = "/multi";

    private final static String VIRTUAL_NODE = "/virtual";

    private final static String LOCKS_PATH_PART = "/locks";

    private final String lockPath;

    private String trimBasePath(String basePath) {
        String newBasePath = null;
        byte ch = basePath.getBytes()[basePath.length() - 1];
        if (ch == '/') {
            newBasePath = basePath.substring(0, basePath.length() - 1);
        } else {
            newBasePath = basePath;
        }
        return newBasePath;
    }

    public ZookeeperIdentification(CuratorClient client, String basePath) {
        this.client = client;
        this.basePath = trimBasePath(basePath);
        this.lockPath = basePath + LOCKS_PATH_PART;
    }

    public String getBasePath() {
        return basePath;
    }

    private void checkPathAndCreate(String singlePath) {
        if (!client.checkExists(singlePath)) {
            client.createPersistent(singlePath, true);
        }
    }

    @Override
    public String getSingleIdentification() {
        checkPathAndCreate(lockPath);

        String singleNode = basePath + SINGLE_NODE;
        ZookeeperIdentificationGen genExecutor = new ZookeeperIdentificationGen(singleNode);
        CuratorLocksClient lockClient = new CuratorLocksClient(client, lockPath, genExecutor, "genSingleIdentification");
        try {
            lockClient.doWork();
        } catch (Exception e) {
            LOG.error("getSingleIdentification error!", e);
        }
        return genExecutor.getServerId();
    }

    @Override
    public String getMultiIndentification() {
        checkPathAndCreate(lockPath);

        String multiNode = basePath + MULTI_NODE;
        ZookeeperIdentificationGen genExecutor = new ZookeeperIdentificationGen(multiNode);
        CuratorLocksClient lockClient = new CuratorLocksClient(client, lockPath, genExecutor, "genMultiIdentification");
        try {
            lockClient.doWork();
        } catch (Exception e) {
            LOG.error("getMultiIndentification error!", e);
        }
        return genExecutor.getServerId();
    }

    @Override
    public String getVirtureIdentification() {

        checkPathAndCreate(lockPath);

        String virtualNode = basePath + VIRTUAL_NODE;
        ZookeeperIdentificationGen genExecutor = new ZookeeperIdentificationGen(virtualNode);
        CuratorLocksClient lockClient = new CuratorLocksClient(client, lockPath, genExecutor, "genVirtualIdentification");
        try {
            lockClient.doWork();
        } catch (Exception e) {
            LOG.error("getVirtureIdentification error!", e);
        }
        return genExecutor.getServerId();

    }

    public static void main(String[] args) throws InterruptedException {
        

        ExecutorService threads = Executors.newFixedThreadPool(10);
        final List<String> sigleServerIdList = new ArrayList<String>();
        final List<String> multiServerIdList = new ArrayList<String>();
        final List<String> virtualServerIdList = new ArrayList<String>();

        for (int i = 0; i < 10; i++) {
            threads.execute(new Runnable() {
                
                @Override
                public void run() {
                    CuratorClient client = CuratorClient.getClientInstance("192.168.101.86:2181");
                    int count = 0;
                    while(count<10) {
                        count++;
                        Identification instance = new ZookeeperIdentification(client, "/brfs/wz/serverID");
                        sigleServerIdList.add(instance.getSingleIdentification());
                        multiServerIdList.add(instance.getMultiIndentification());
                        virtualServerIdList.add(instance.getVirtureIdentification());
                    }
                    
                }
            });
        }
        
        threads.shutdown();
        threads.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);
        
        Set<String> singleSet = new HashSet<>();
        Set<String> multiSet = new HashSet<>();
        Set<String> virtualSet = new HashSet<>();
        singleSet.addAll(sigleServerIdList);
        multiSet.addAll(multiServerIdList);
        virtualSet.addAll(virtualServerIdList);
        
        System.out.println(sigleServerIdList.size() + "--" + singleSet.size());
        
        System.out.println(multiServerIdList.size() + "--" + multiSet.size());
        
        System.out.println(virtualServerIdList.size() + "--" + virtualSet.size());
        
    }

}
