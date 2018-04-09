package com.bonree.brfs.common.zookeeper.curator.cache;

import org.apache.curator.framework.recipes.cache.NodeCacheListener;

public abstract class AbstractNodeCacheListener implements NodeCacheListener {

    private final static String DEFAULT_NAME = "default";

    private String listenName;

    public AbstractNodeCacheListener(String listenName) {
        if (listenName == null) {
            this.listenName = DEFAULT_NAME;
        } else {
            this.listenName = listenName;
        }
    }

    public String getListenName() {
        return listenName;
    }
}
