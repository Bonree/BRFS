package com.bonree.brfs.common.zookeeper.curator.leader;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.curator.framework.recipes.leader.LeaderSelector;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bonree.brfs.common.zookeeper.curator.CuratorClient;
import com.bonree.brfs.configuration.Configuration;

public class CuratorLeaderSelector {

    private final static Logger LOG = LoggerFactory.getLogger(CuratorLeaderSelector.class);

    private volatile static CuratorLeaderSelector selector = null;

    private Map<String, List<LeaderSelector>> selectorMap;

    private CuratorClient client = null;

    private CuratorLeaderSelector(String zkUrl) {
        client = CuratorClient.getClientInstance(zkUrl);
        selectorMap = new ConcurrentHashMap<String, List<LeaderSelector>>();
    }

    public static CuratorLeaderSelector getLeaderSelectorInstance(String zkUrl) {

        LOG.info("create CuratorLeaderSelector...");
        if (selector == null) {
            synchronized (Configuration.class) {
                if (selector == null) {
                    selector = new CuratorLeaderSelector(zkUrl);
                }
            }
        }
        return selector;
    }

    public synchronized void addSelector(String path, LeaderSelectorListener listener) {
        List<LeaderSelector> leaderSelectors = selectorMap.get(path);
        if (leaderSelectors == null) {
            leaderSelectors = new ArrayList<LeaderSelector>();
            LeaderSelector leaderSelector = new LeaderSelector(client.getInnerClient(), path, listener);
            leaderSelector.autoRequeue();
            leaderSelectors.add(leaderSelector);
            leaderSelector.start();
            selectorMap.put(path, leaderSelectors);
        } else {
            LeaderSelector leaderSelector = new LeaderSelector(client.getInnerClient(), path, listener);
            leaderSelector.autoRequeue();
            leaderSelectors.add(leaderSelector);
            leaderSelector.start();
            selectorMap.put(path, leaderSelectors);
        }
    }

//    public void removeSelector(String path) {
//        LeaderSelector leaderSelector = selectorMap.get(path);
//        if (leaderSelector != null) {
//            leaderSelector.close();
//            selectorMap.remove(path);
//        }
//    }
//
//    public LeaderSelector getSelectorListener(String path) {
//        LeaderSelector leaderSelector = selectorMap.get(path);
//        if (leaderSelector != null) {
//
//        }
//        return null;
//    }

}
