package com.bonree.brfs.duplication.datastream.dataengine.impl;

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
import com.bonree.brfs.duplication.storagename.StorageNameManager;
import com.bonree.brfs.duplication.storagename.StorageNameNode;
import com.bonree.brfs.duplication.storagename.StorageNameStateListener;
import com.google.common.base.Optional;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;

public class DefaultDataEngineManager implements DataEngineManager {
	private static final Logger LOG = LoggerFactory.getLogger(DefaultDataEngineManager.class);
	
	private StorageNameManager storageNameManager;
	private DataEngineFactory storageRegionFactory;
	
	private LoadingCache<Integer, Optional<DataEngine>> dataEngineContainer;
	
	public DefaultDataEngineManager(StorageNameManager storageNameManager, DataEngineFactory factory) {
		this(storageNameManager, factory,
				Duration.parse(Configs.getConfiguration().GetConfig(DuplicateNodeConfigs.CONFIG_DATA_ENGINE_IDLE_TIME)));
	}
	
	public DefaultDataEngineManager(StorageNameManager storageNameManager, DataEngineFactory factory, Duration idleTime) {
		this.storageNameManager = storageNameManager;
		this.storageRegionFactory = factory;
		this.dataEngineContainer = CacheBuilder.newBuilder()
				.expireAfterAccess(idleTime.toMillis(), TimeUnit.MILLISECONDS)
				.removalListener(new StorageRegionRemovalListener())
				.build(new DataEngineLoader());
		
		this.storageNameManager.addStorageNameStateListener(new StorageRegionStateHandler());
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
			StorageNameNode storageRegion = storageNameManager.findStorageName(storageRegionId);
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
	
	private class StorageRegionStateHandler implements StorageNameStateListener {

		@Override
		public void storageNameAdded(StorageNameNode node) {}

		@Override
		public void storageNameUpdated(StorageNameNode node) {}

		@Override
		public void storageNameRemoved(StorageNameNode node) {
			LOG.info("Storage region[{},{}] is removed!", node.getName(), node.getId());
			dataEngineContainer.invalidate(node.getId());
		}
		
	}
}
