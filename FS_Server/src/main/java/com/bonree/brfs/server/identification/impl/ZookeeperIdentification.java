package com.bonree.brfs.server.identification.impl;

import com.bonree.brfs.server.identification.Identification;
import com.bonree.brfs.zookeeper.curator.CuratorZookeeperClient;

public class ZookeeperIdentification implements Identification {

    private final String basePath;
    private CuratorZookeeperClient client;

    private final static String SINGLE_NODE = "/single";

    private final static String MULTI_NODE = "/multi";

    private final static String VIRTUAL_NODE = "/virtual";
    

    public ZookeeperIdentification(String basePath) {
        byte ch = basePath.getBytes()[basePath.length() - 1];
        if (ch == '/') {
            this.basePath = basePath.substring(0, basePath.length() - 1);
        } else {
            this.basePath = basePath;
        }
        this.client = client;

    }

    public String getBasePath() {
        return basePath;
    }

    @Override
    public String getSingleIdentification() {
        String singlePath = basePath + SINGLE_NODE;
        if (!client.checkExists(singlePath)) {
            client.createPersistent(singlePath, true);
        }
        
        return null;
    }

    @Override
    public String getMultiIndentification() {

        return null;
    }

    @Override
    public String getVirtureIdentification() {

        return null;
    }

    public static void main(String[] args) {
        String testStr = "/brfs/wz/serverID";
        String tmp = null;
        int size = testStr.length();
        System.out.println(size);
        byte aaa = testStr.getBytes()[size - 1];
        if (aaa == '/') {
            tmp = testStr.substring(0, size - 1);
        }
        System.out.println(tmp);
    }

}
