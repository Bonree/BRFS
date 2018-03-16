package com.bonree.brfs.common.zookeeper.curator;

import java.io.IOException;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListener;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListenerAdapter;

import com.bonree.brfs.common.zookeeper.curator.leader.AbstractLeaderSelectorListener;
import com.bonree.brfs.common.zookeeper.curator.leader.CuratorLeaderSelector;

public class LeaderTest {

    public static void main(String[] args) throws InterruptedException, IOException {
        String path = "/brfs/wz/leader";
        LeaderSelectorListener leaderSelector1 = new LeaderSelectorListenerAdapter() {

            @Override
            public void takeLeadership(CuratorFramework client) throws Exception {
                // TODO Auto-generated method stub

            }
        };

        LeaderSelectorListener leaderSelector2 = new LeaderSelectorListenerAdapter() {

            @Override
            public void takeLeadership(CuratorFramework client) throws Exception {
                // TODO Auto-generated method stub

            }
        };

        CuratorLeaderSelector leaderSelector = CuratorLeaderSelector.getLeaderSelectorInstance("192.168.101.86:2181");
        
        leaderSelector.addAndStartSelector(path, leaderSelector1);
        Thread.sleep(Long.MAX_VALUE);

    }

}
