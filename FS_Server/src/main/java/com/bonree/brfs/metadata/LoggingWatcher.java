package com.bonree.brfs.metadata;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Watcher implementation that simply logs at debug.
 */
public class LoggingWatcher implements Watcher {
    private static final Logger LOG = LoggerFactory.getLogger(LoggingWatcher.class);

    @Override
    public void process(WatchedEvent event) {
        LOG.debug("Ignoring watched event: " + event);
    }
}