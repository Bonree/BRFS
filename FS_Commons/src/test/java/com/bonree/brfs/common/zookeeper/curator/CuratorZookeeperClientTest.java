package com.bonree.brfs.common.zookeeper.curator;

import junit.framework.TestCase;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryNTimes;

/*******************************************************************************
 * 版权信息：博睿宏远科技发展有限公司
 * Copyright: Copyright (c) 2007博睿宏远科技发展有限公司,Inc.All Rights Reserved.
 *
 * @date 2018年3月12日 下午6:40:06
 * @Author: <a href=mailto:weizheng@bonree.com>魏征</a>
 * @Description:
 ******************************************************************************/
public class CuratorZookeeperClientTest extends TestCase {

    private static final String zkUrl = "192.168.4.114:2181";

    private static final RetryPolicy retry = new RetryNTimes(1, 1000);

    private final int sessionTimeoutMs = 5000;

    private final int connectionTimeoutMs = 15000;

    private boolean isWaitConnection = true;

    public static void main(String[] args) throws Exception {

        //        CuratorClient client = CuratorClient.getClientInstance(zkUrl);
        //        client.useNameSpace("aaa");
        //        client.createPersistent("/bbb",true,"wz".getBytes(StandardCharsets.UTF_8));
        try {

            CuratorFramework client = CuratorFrameworkFactory.newClient(zkUrl, retry);
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
        } catch (Exception e) {
            // ignore
        }

    }

}
