package com.bonree.brfs.metadata.backup;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

class ReaderThread extends Thread implements Watcher {

    private ZooKeeper zk;

    ReaderThread(Runnable r, ZooKeeper zk) {
        super(r);
        this.zk = zk;
    }

    @Override
    public void process(WatchedEvent event) {
        // TODO Auto-generated method stub
    }

    public ZooKeeper getZooKeeper() {
        return zk;
    }
}
