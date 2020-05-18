package com.bonree.brfs.duplication.datastream.dataengine.impl;

import com.bonree.brfs.common.lifecycle.LifecycleStop;
import com.bonree.brfs.common.lifecycle.ManageLifecycle;
import com.bonree.brfs.configuration.Configs;
import com.bonree.brfs.configuration.units.RegionNodeConfigs;
import com.bonree.brfs.duplication.datastream.dataengine.DataEngine;
import com.bonree.brfs.duplication.datastream.dataengine.DataEngineFactory;
import com.bonree.brfs.duplication.datastream.dataengine.DataEngineManager;
import com.bonree.brfs.duplication.storageregion.StorageRegion;
import com.bonree.brfs.duplication.storageregion.StorageRegionManager;
import com.bonree.brfs.duplication.storageregion.StorageRegionStateListener;
import com.bonree.brfs.duplication.storageregion.exception.StorageRegionNonexistentException;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import java.io.Closeable;
import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import javax.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ManageLifecycle
public class DefaultDataEngineManager implements DataEngineManager, Closeable {
    private static final Logger LOG = LoggerFactory.getLogger(DefaultDataEngineManager.class);

    private StorageRegionManager storageRegionManager;
    private DataEngineFactory storageRegionFactory;

    private LoadingCache<String, DataEngine> dataEngineContainer;

    @Inject
    public DefaultDataEngineManager(StorageRegionManager storageRegionManager, DataEngineFactory factory) {
        this(storageRegionManager, factory,
             Duration.parse(Configs.getConfiguration().getConfig(RegionNodeConfigs.CONFIG_DATA_ENGINE_IDLE_TIME)));
    }

    public DefaultDataEngineManager(StorageRegionManager storageRegionManager, DataEngineFactory factory,
                                    Duration idleTime) {
        this.storageRegionManager = storageRegionManager;
        this.storageRegionFactory = factory;
        this.dataEngineContainer = CacheBuilder.newBuilder()
                                               .expireAfterAccess(idleTime.toMillis(), TimeUnit.MILLISECONDS)
                                               .removalListener(new StorageRegionRemovalListener()).build(new DataEngineLoader());

        this.storageRegionManager.addStorageRegionStateListener(new StorageRegionStateHandler());
    }

    @Override
    public DataEngine getDataEngine(String srName) {
        try {
            return dataEngineContainer.get(srName);
        } catch (ExecutionException e) {
            LOG.error("get dataEngine of sr[{}] error.", srName, e.getCause());
        }

        return null;
    }

    private class DataEngineLoader extends CacheLoader<String, DataEngine> {

        @Override
        public DataEngine load(String srName) throws Exception {
            StorageRegion storageRegion = storageRegionManager.findStorageRegionByName(srName);
            if (storageRegion == null) {
                throw new StorageRegionNonexistentException("sr[" + srName + "]");
            }

            return storageRegionFactory.createDataEngine(storageRegion);
        }
    }

    private class StorageRegionRemovalListener implements RemovalListener<String, DataEngine> {

        @Override
        public void onRemoval(RemovalNotification<String, DataEngine> notification) {
            LOG.info("closing dataEngine of sr[{}]...", notification.getKey());
            DataEngine dataEngine = notification.getValue();
            try {
                dataEngine.close();
            } catch (IOException e) {
                LOG.error("close dataEngine of sr[{}] failed", notification.getKey());
            }
        }

    }

    private class StorageRegionStateHandler implements StorageRegionStateListener {

        @Override
        public void storageRegionAdded(StorageRegion node) {
        }

        @Override
        public void storageRegionUpdated(StorageRegion node) {
            // Storage Region属性的变化也许要重新加载Data Engine
            LOG.info("Storage region[{},{}] is updated!", node.getName(), node.getId());
            dataEngineContainer.invalidate(node.getId());
        }

        @Override
        public void storageRegionRemoved(StorageRegion node) {
            LOG.info("Storage region[{},{}] is removed!", node.getName(), node.getId());
            dataEngineContainer.invalidate(node.getId());
        }

    }

    @LifecycleStop
    @Override
    public void close() throws IOException {
        if (dataEngineContainer == null) {
            return;
        }

        dataEngineContainer.invalidateAll();
    }

}
