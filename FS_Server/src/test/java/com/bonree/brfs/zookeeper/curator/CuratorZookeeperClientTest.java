package com.bonree.brfs.zookeeper.curator;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.BackgroundCallback;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.framework.api.CuratorListener;
import org.apache.curator.retry.RetryNTimes;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;

import junit.framework.TestCase;

/*******************************************************************************
 * 版权信息：博睿宏远科技发展有限公司
 * Copyright: Copyright (c) 2007博睿宏远科技发展有限公司,Inc.All Rights Reserved.
 * 
 * @date 2018年3月12日 下午6:40:06
 * @Author: <a href=mailto:weizheng@bonree.com>魏征</a>
 * @Description: 
 ******************************************************************************/
public class CuratorZookeeperClientTest extends TestCase {

    private final static String zkUrl = "192.168.101.86:2181";

    private final static RetryPolicy retry = new RetryNTimes(1, 1000);

    private final int sessionTimeoutMs = 5000;

    private final int connectionTimeoutMs = 15000;

    private boolean isWaitConnection = true;

    public void testGetClientInstance() throws Exception {
        CuratorZookeeperClient client = CuratorZookeeperClient.getClientInstance(zkUrl, retry, sessionTimeoutMs, connectionTimeoutMs, isWaitConnection);
        assertNotNull(client);
        client.close();
    }

    public void testCurd() throws Exception {
        boolean flag = false;
        CuratorZookeeperClient client = CuratorZookeeperClient.getClientInstance(zkUrl);

        client.createEphemeral("/brfs/wz/test/createEphemeral", true);
        flag = client.checkExists("/brfs/wz/test/createEphemeral");
        assertEquals(flag, true);

        client.createPersistent("/brfs/wz/test/createPersistent", true);
        flag = client.checkExists("/brfs/wz/test/createPersistent");
        assertEquals(flag, true);

        client.guaranteedDelete("/brfs/wz/test/createPersistent", false);
        flag = client.checkExists("/brfs/wz/test/createPersistent");
        assertEquals(flag, false);

        client.setData("/brfs/wz/test/createEphemeral", "createEphemeral".getBytes());

        assertEquals("createEphemeral", new String(client.getData("/brfs/wz/test/createEphemeral")));

        client.close();
    }

    public void testWatcher() throws Exception {

        ExecutorService serverThreads = Executors.newFixedThreadPool(10);
        final CuratorZookeeperClient client = CuratorZookeeperClient.getClientInstance(zkUrl);
        if (!client.checkExists("/brfs/wz/servers")) {
            client.createPersistent("/brfs/wz/servers", true);
        }
        MyWatcher watcher = new MyWatcher(client);
        System.out.println(client.watchedGetChildren("/brfs/wz/servers", watcher));

        for (int i = 0; i < 10; i++) {
            final int count = i;
            serverThreads.execute(new Runnable() {
                //
                @Override
                public void run() {
                    synchronized (client) {
                        client.createEphemeral("/brfs/wz/servers/server" + count, true);
                    }
                }
            });
        }
        serverThreads.shutdown();
        serverThreads.awaitTermination(1, TimeUnit.DAYS);
        client.close();
    }

    public class MyWatcher implements Watcher {

        private final CuratorZookeeperClient client;

        public MyWatcher(CuratorZookeeperClient client) {

            this.client = client;
        }

        @Override
        public void process(WatchedEvent event) {
            if (event.getType() == EventType.NodeChildrenChanged) {
                List<String> tmps = client.watchedGetChildren(event.getPath(), this);
                System.out.println(1111);
                System.out.println(tmps);
            }
        }

    }

    public void testCuratorListener() throws Exception {
        final CuratorZookeeperClient client = CuratorZookeeperClient.getClientInstance(zkUrl);
        CuratorFramework curatorClient = client.getInnerClient();

        System.out.println("list:" + curatorClient.getChildren().inBackground(new BackgroundCallback() {

            @Override
            public void processResult(CuratorFramework client, CuratorEvent event) throws Exception {
                System.out.println(1111);
                System.out.println(event.getName());
                System.out.println(event.getPath());
                System.out.println(event.getStat());
                System.out.println("aaaaa" + event.getChildren());
            }
        }).forPath("/brfs/wz"));

        curatorClient.getCuratorListenable().addListener(new CuratorListener() {

            @Override
            public void eventReceived(CuratorFramework client, CuratorEvent event) throws Exception {
                System.out.println("CuratorListener1");

            }
        });

        curatorClient.getCuratorListenable().addListener(new CuratorListener() {

            @Override
            public void eventReceived(CuratorFramework client, CuratorEvent event) throws Exception {
                System.out.println("CuratorListener2");

            }
        });

        curatorClient.create().inBackground().forPath("/brfs/wz/test2");

        Thread.sleep(Long.MAX_VALUE);
        client.close();

    }

}
