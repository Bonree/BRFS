package com.bonree.brfs.client;

import com.bonree.brfs.client.route.ServiceSelectorManager;
import com.bonree.brfs.common.ZookeeperPaths;
import com.bonree.brfs.common.service.ServiceManager;
import com.bonree.brfs.common.service.impl.DefaultServiceManager;
import com.bonree.brfs.common.zookeeper.curator.CuratorClient;

public class BrfsClient {

    public static void main(String[] args) throws Exception {
        CuratorClient client = CuratorClient.getClientInstance("192.168.111.13:2181");
        ZookeeperPaths zkPaths = ZookeeperPaths.create("test1", "192.168.111.13:2181");

        ServiceManager sm = new DefaultServiceManager(client.getInnerClient().usingNamespace(zkPaths.getBaseClusterName().substring(1)));
        sm.start();

//        ServiceSelectorManager selectorManager = new ServiceSelectorManager(sm, client.getInnerClient(), zkPaths.getBaseServerIdPath(), zkPaths.getBaseRoutePath());

        // Thread.sleep(3000);
        // System.out.println("write select server:"+selectorManager.useStorageIndex(1).writerService());
        // System.out.println("random select server:"+selectorManager.useStorageIndex(1).randomService());
        // ServiceSelectorCache cache = selectorManager.useStorageIndex(10);
        // System.out.println("read select server:"+cache.readerService("B82B5A90189348888C078469B9D8B24F_22").getFirstServer());
//        System.out.println("read select server:" + selectorManager.useDiskSelector(4).readerService("33F264BCF35A4C68BADB285E4516ECF9_28_29").getFirstServer());
//         System.out.println("write select server:" + selectorManager.useDuplicaSelector().randomService());
//         System.out.println("random select server:" + selectorManager.useDuplicaSelector().randomService());
       
        // System.out.println("index write select server:" + selectorManager.useDiskSelector(10).randomService());
       Thread.sleep(3000);
       
//        selectorManager.close();
       sm.addServiceStateListener("1111", null);
        sm.stop();
        client.close();
    }

}
