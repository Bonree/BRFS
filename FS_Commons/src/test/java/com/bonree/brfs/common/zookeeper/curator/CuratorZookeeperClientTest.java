package com.bonree.brfs.common.zookeeper.curator;

import junit.framework.TestCase;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryNTimes;

import java.nio.charset.StandardCharsets;

/*******************************************************************************
 * 版权信息：博睿宏远科技发展有限公司
 * Copyright: Copyright (c) 2007博睿宏远科技发展有限公司,Inc.All Rights Reserved.
 *
 * @date 2018年3月12日 下午6:40:06
 * @Author: <a href=mailto:weizheng@bonree.com>魏征</a>
 * @Description:
 ******************************************************************************/
public class CuratorZookeeperClientTest extends TestCase {

    private final static String zkUrl = "192.168.4.114:2181";

    private final static RetryPolicy retry = new RetryNTimes(1, 1000);

    private final int sessionTimeoutMs = 5000;

    private final int connectionTimeoutMs = 15000;

    private boolean isWaitConnection = true;

//    public void testGetClientInstance() throws Exception {
//        CuratorZookeeperClient client = CuratorZookeeperClient.getClientInstance(zkUrl, retry, sessionTimeoutMs, connectionTimeoutMs, isWaitConnection);
//        assertNotNull(client);
//        client.close();
//    }
//
//    public void testCurd() throws Exception {
//        boolean flag = false;
//        CuratorZookeeperClient client = CuratorZookeeperClient.getClientInstance(zkUrl);
//
//        client.createEphemeral("/brfs/wz/test/createEphemeral", true);
//        flag = client.checkExists("/brfs/wz/test/createEphemeral");
//        assertEquals(flag, true);
//
//        client.createPersistent("/brfs/wz/test/createPersistent", true);
//        flag = client.checkExists("/brfs/wz/test/createPersistent");
//        assertEquals(flag, true);
//
//        client.guaranteedDelete("/brfs/wz/test/createPersistent", false);
//        flag = client.checkExists("/brfs/wz/test/createPersistent");
//        assertEquals(flag, false);
//
//        client.setData("/brfs/wz/test/createEphemeral", "createEphemeral".getBytes());
//
//        assertEquals("createEphemeral", new String(client.getData("/brfs/wz/test/createEphemeral")));
//
//        client.close();
//    }
//
//    public void testWatcher() throws Exception {
//
//        ExecutorService serverThreads = Executors.newFixedThreadPool(10);
//        final CuratorZookeeperClient client = CuratorZookeeperClient.getClientInstance(zkUrl);
//        if (!client.checkExists("/brfs/wz/servers")) {
//            client.createPersistent("/brfs/wz/servers", true);
//        }
//        MyWatcher watcher = new MyWatcher(client);
//        System.out.println(client.watchedGetChildren("/brfs/wz/servers", watcher));
//
//        for (int i = 0; i < 10; i++) {
//            final int count = i;
//            serverThreads.execute(new Runnable() {
//                //
//                @Override
//                public void run() {
//                    synchronized (client) {
//                        client.createEphemeral("/brfs/wz/servers/server" + count, true);
//                    }
//                }
//            });
//        }
//        serverThreads.shutdown();
//        serverThreads.awaitTermination(1, TimeUnit.DAYS);
//        client.close();
//    }
//
//    public class MyWatcher implements Watcher {
//
//        private final CuratorZookeeperClient client;
//
//        public MyWatcher(CuratorZookeeperClient client) {
//
//            this.client = client;
//        }
//
//        @Override
//        public void process(WatchedEvent event) {
//            if (event.getType() == EventType.NodeChildrenChanged) {
//                List<String> tmps = client.watchedGetChildren(event.getPath(), this);
//                System.out.println(1111);
//                System.out.println(tmps);
//            }
//        }
//
//    }

    //    public void testCuratorListener() throws Exception {
//        final CuratorClient client = CuratorClient.getClientInstance(zkUrl);
//        CuratorFramework curatorClient = client.getInnerClient();
////        curatorClient.getChildren().inBackground(new BackgroundCallback() {
////
////            @Override
////            public void processResult(CuratorFramework client, CuratorEvent event) throws Exception {
////                System.out.println("aaaaa" + event.getChildren());
////            }
////        }).forPath("/brfs/wz");
//
//        curatorClient.getCuratorListenable().addListener(new CuratorListener() {
//
//            @Override
//            public void eventReceived(CuratorFramework client, CuratorEvent event) throws Exception {
//                System.out.println("CuratorListener1--" + event.getPath()+"--" + event.getWatchedEvent());
//
//            }
//        });
//
//        curatorClient.getCuratorListenable().addListener(new CuratorListener() {
//
//            @Override
//            public void eventReceived(CuratorFramework client, CuratorEvent event) throws Exception {
//                System.out.println("CuratorListener2--" + event.getPath()+"--" + event.getWatchedEvent());
//            }
//        });
//
//        curatorClient.setData().inBackground().forPath("/yupeng/yupeng/yupeng","aaa".getBytes());
//
//        Thread.sleep(2000);
//        client.close();
//
//    }
    public static void main(String[] args) throws Exception {

//        CuratorClient client = CuratorClient.getClientInstance(zkUrl);
//        client.useNameSpace("aaa");
//        client.createPersistent("/bbb",true,"wz".getBytes(StandardCharsets.UTF_8));
        try {

            CuratorFramework client = CuratorFrameworkFactory.newClient(zkUrl,retry);
            client.start();
//            client = client.usingNamespace("1");
//            client.create().creatingParentsIfNeeded().forPath("/bbb");
//            client = client.usingNamespace("2/bbb");
//            client.create().creatingParentsIfNeeded().forPath("/aaa");
//            client = client.usingNamespace("3");
//            client.create().creatingParentsIfNeeded().forPath("/aaa");
//            client = client.usingNamespace("4");
//            Thread.sleep(60000);

            client.create().creatingParentsIfNeeded().forPath("/aaa");
            client.close();
        }catch (Exception e){

        }


    }


}
