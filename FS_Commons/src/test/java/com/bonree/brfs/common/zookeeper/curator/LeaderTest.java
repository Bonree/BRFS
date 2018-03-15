package com.bonree.brfs.common.zookeeper.curator;

import java.io.IOException;

import org.apache.curator.framework.CuratorFramework;

import com.bonree.brfs.common.zookeeper.curator.leader.AbstractLeaderSelectorClient;

public class LeaderTest {

    public static void main(String[] args) throws InterruptedException, IOException {
        String path = "/brfs/wz/leader";
        CuratorZookeeperClient client1 = CuratorZookeeperClient.getClientInstance("192.168.101.86:2181");
        
        AbstractLeaderSelectorClient leaderSelector1 = new AbstractLeaderSelectorClient("testClient1", client1.getInnerClient(), path) {

            @Override
            public void takeLeadership(CuratorFramework client) throws Exception {
                System.out.println(this.getClientName() + " is leadership");
                Thread.sleep(5000);
                System.out.println(this.getClientName() + " will release leadership");
                System.out.println("1 second after , that will reselect leadership");
                Thread.sleep(1000);
            }
        };
        
        CuratorZookeeperClient client2 = CuratorZookeeperClient.getClientInstance("192.168.101.86:2181");
        AbstractLeaderSelectorClient leaderSelector2 = new AbstractLeaderSelectorClient("testClient2", client2.getInnerClient(), path) {

            @Override
            public void takeLeadership(CuratorFramework client) throws Exception {
                System.out.println(this.getClientName() + " is leadership");
                Thread.sleep(5000);
                System.out.println(this.getClientName() + " will release leadership");
                System.out.println("1 second after , that will reselect leadership");
                Thread.sleep(1000);
            }
        };
        
        leaderSelector1.start();
        leaderSelector2.start();
        
        Thread.sleep(Long.MAX_VALUE);
        
        //调用此方法，则意味着该client退出选举
        leaderSelector1.close();
        leaderSelector2.close();
        client1.close();
        client2.close();
    }

}
