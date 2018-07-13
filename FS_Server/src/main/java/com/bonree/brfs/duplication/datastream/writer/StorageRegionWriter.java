package com.bonree.brfs.duplication.datastream.writer;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReferenceArray;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bonree.brfs.common.write.data.DataItem;
import com.bonree.brfs.duplication.datastream.dataengine.DataEngine;
import com.bonree.brfs.duplication.datastream.dataengine.DataEngineManager;
import com.bonree.brfs.duplication.datastream.dataengine.DataStoreCallback;

public class StorageRegionWriter {
	private static final Logger LOG = LoggerFactory.getLogger(StorageRegionWriter.class);
	
	private DataEngineManager dataEngineManager;
	
	public StorageRegionWriter(DataEngineManager dataEngineManager) {
		this.dataEngineManager = dataEngineManager;
	}
	
	public void write(int storageRegionId, DataItem[] items, StorageRegionWriteCallback callback) {
		DataEngine dataEngine = dataEngineManager.getDataEngine(storageRegionId);
		if(dataEngine == null) {
			LOG.error("can not get data engine by region[id={}]", storageRegionId);
			callback.error();
			return;
		}
		
		AtomicReferenceArray<String> fids = new AtomicReferenceArray<String>(items.length);
		AtomicInteger count = new AtomicInteger(items.length);
		for(int i = 0; i < items.length; i++) {
			dataEngine.store(items[i].getBytes(), new DataCallback(i, fids, count, callback));
		}
	}
	
	private static class DataCallback implements DataStoreCallback {
		private final int index;
		private AtomicReferenceArray<String> fids;
		private AtomicInteger count;
		private StorageRegionWriteCallback callback;
		
		public DataCallback(int index, AtomicReferenceArray<String> fids, AtomicInteger count, StorageRegionWriteCallback callback) {
			this.index = index;
			this.fids = fids;
			this.count = count;
			this.callback = callback;
		}

		@Override
		public void dataStored(String storeToken) {
			fids.set(index, storeToken);
			
			if(count.decrementAndGet() == 0) {
				String[] results = new String[fids.length()];
				for(int i = 0; i < results.length; i++) {
					results[i] = fids.get(i);
				}
				
				callback.complete(results);
			}
		}
		
	}
}
