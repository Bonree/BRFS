package com.bonree.brfs.tasks.monitor.impl;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryNTimes;
import org.junit.Before;
import org.junit.Test;

public class RebalanceTaskDestoryerTest {
    private String zkAddress = "localhost:2181";
    private CuratorFramework client = null;
    private String testZkPath = "/brfs/test/rebalance";
    @Before
    public void init()throws Exception{
        client = CuratorFrameworkFactory.newClient(zkAddress,new RetryNTimes(5,1000));
        client.start();
        client.blockUntilConnected();
    }
    @Test
    public void constructTest(){
        RebalanceTaskDestoryer monitor = new RebalanceTaskDestoryer(client,testZkPath,1);
        monitor.start();
        for (int i = 0 ;i<100000;i++){
            System.out.println(i+"--"+monitor.isExecute());
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        monitor.stop();
    }
}
