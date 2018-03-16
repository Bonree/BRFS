package com.bonree.brfs.common.zookeeper.curator.leader;

import java.io.Closeable;
import java.io.IOException;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.leader.LeaderSelector;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListenerAdapter;

public abstract class AbstractLeaderSelectorClient extends LeaderSelectorListenerAdapter implements Closeable {

    private final String clientName;
    
    private final LeaderSelector leaderSelector;

    public AbstractLeaderSelectorClient(String clientName, CuratorFramework client, String path) {
        this.clientName = clientName;
        leaderSelector = new LeaderSelector(client, path, this);
        leaderSelector.autoRequeue();
    }

    public void start() {
        // the selection for this instance doesn't start until the leader selector is started
        // leader selection is done in the background so this call to leaderSelector.start() returns immediately
        leaderSelector.start();
    }

    @Override
    public void close() throws IOException {
        leaderSelector.close();
    }

    public String getClientName() {
        return clientName;
    }

}
