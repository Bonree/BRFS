package com.bonree.brfs.common.zookeeper.curator;

import java.io.IOException;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListener;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListenerAdapter;

import com.bonree.brfs.common.zookeeper.curator.leader.CuratorLeaderSelectorClient;

public class LeaderTest {
    public static class MyLeaderListener extends LeaderSelectorListenerAdapter {

        private String listenerName;

        public MyLeaderListener(String listenerName) {
            this.listenerName = listenerName;
        }

        // 身为leader时，会执行该函数的代码，执行完毕后，会放弃leader。并会参与下次竞选
        @Override
        public void takeLeadership(CuratorFramework client) throws Exception {
            System.out.println(listenerName + ": I am the leader.");
            System.out.println(listenerName + ": 5 seconds,I relinquish the leader");
            Thread.sleep(5000);
            System.out.println("reselect the leader!");
        }
    };

    public static void main(String[] args) throws InterruptedException, IOException {
        String path = "/brfs/wz/leader";

        LeaderSelectorListener l1 = new MyLeaderListener("listener1");
        LeaderSelectorListener l2 = new MyLeaderListener("listener2");

        CuratorLeaderSelectorClient leaderSelector = CuratorLeaderSelectorClient.getLeaderSelectorInstance("192.168.101.86:2181");
        leaderSelector.addSelector(path, l1);
        leaderSelector.addSelector(path, l2);

        // Thread.sleep(Long.MAX_VALUE);
        Thread.sleep(10000);

        System.out.println(leaderSelector.getSelectorListeners(path));

        Thread.sleep(30000);

        leaderSelector.removeAllSelector(path);

        Thread.sleep(Long.MAX_VALUE);

    }

}
