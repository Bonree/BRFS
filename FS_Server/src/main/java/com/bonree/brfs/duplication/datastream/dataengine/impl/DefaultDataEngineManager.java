package com.bonree.brfs.duplication.datastream.dataengine.impl;

import java.io.Closeable;
import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bonree.brfs.configuration.Configs;
import com.bonree.brfs.configuration.units.DuplicateNodeConfigs;
import com.bonree.brfs.duplication.datastream.dataengine.DataEngine;
import com.bonree.brfs.duplication.datastream.dataengine.DataEngineFactory;
import com.bonree.brfs.duplication.datastream.dataengine.DataEngineManager;
import com.bonree.brfs.duplication.storageregion.StorageRegion;
import com.bonree.brfs.duplication.storageregion.StorageRegionManager;
import com.bonree.brfs.duplication.storageregion.StorageRegionStateListener;
import com.google.common.base.Optional;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;

public class DefaultDataEngineManager implements DataEngineManager, Closeable {
	private static final Logger LOG = LoggerFactory.getLogger(DefaultDataEngineManager.class);
	
	private StorageRegionManager storageRegionManager;
	private DataEngineFactory storageRegionFactory;
	
	private LoadingCache<Integer, Optional<DataEngine>> dataEngineContainer;
	
	public DefaultDataEngineManager(StorageRegionManager storageRegionManager, DataEngineFactory factory) {
		this(storageRegionManager, factory,
				Duration.parse(Configs.getConfiguration().GetConfig(DuplicateNodeConfigs.CONFIG_DATA_ENGINE_IDLE_TIME)));
	}
	
	public DefaultDataEngineManager(StorageRegionManager storageRegionManager, DataEngineFactory factory, Duration idleTime) {
		this.storageRegionManager = storageRegionManager;
		this.storageRegionFactory = factory;
		this.dataEngineContainer = CacheBuilder.newBuilder()
				.expireAfterAccess(idleTime.toMillis(), TimeUnit.MILLISECONDS)
				.removalListener(new StorageRegionRemovalListener())
				.build(new DataEngineLoader());
		
		this.storageRegionManager.addStorageRegionStateListener(new StorageRegionStateHandler());
	}

	@Override
	public DataEngine getDataEngine(int baseId) {
		try {
			Optional<DataEngine> optional = dataEngineContainer.get(baseId);
			
			if(!optional.isPresent()) {
				dataEngineContainer.invalidate(baseId);
			}
			
			return optional.orNull();
		} catch (ExecutionException e) {
			LOG.error("get dataEngine by id[{}] error.", baseId, e);
		}
		
		return null;
	}

	private class DataEngineLoader extends CacheLoader<Integer, Optional<DataEngine>> {

		@Override
		public Optional<DataEngine> load(Integer storageRegionId) throws Exception {
			StorageRegion storageRegion = storageRegionManager.findStorageRegionById(storageRegionId);
			if(storageRegion == null) {
				return Optional.absent();
			}
			
			return Optional.fromNullable(storageRegionFactory.createDataEngine(storageRegion));
		}
		
	}
	
	private class StorageRegionRemovalListener implements RemovalListener<Integer, Optional<DataEngine>> {

		@Override
		public void onRemoval(RemovalNotification<Integer, Optional<DataEngine>> notification) {
			Optional<DataEngine> optional = notification.getValue();
			if(optional.isPresent()) {
				LOG.info("closing dataEngine[id={}]...", notification.getKey());
				DataEngine dataEngine = optional.get();
				try {
					dataEngine.close();
				} catch (IOException e) {
					LOG.error("close dataEngine[id={}] failed", notification.getKey());
				}
			}
		}
		
	}
	
	private class StorageRegionStateHandler implements StorageRegionStateListener {

		@Override
		public void storageRegionAdded(StorageRegion node) {}

		@Override
		public void storageRegionUpdated(StorageRegion node) {
			//Storage Region属性的变化也许要重新加载Data Engine
			LOG.info("Storage region[{},{}] is updated!", node.getName(), node.getId());
			dataEngineContainer.invalidate(node.getId());
		}

		@Override
		public void storageRegionRemoved(StorageRegion node) {
			LOG.info("Storage region[{},{}] is removed!", node.getName(), node.getId());
			dataEngineContainer.invalidate(node.getId());
		}
		
	}
	

	@Override
	public void close() throws IOException {
		if(dataEngineContainer == null) {
			return;
		}
		
		dataEngineContainer.invalidateAll();
	}
}
