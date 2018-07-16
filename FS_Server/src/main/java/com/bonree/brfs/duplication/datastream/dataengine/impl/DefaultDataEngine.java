package com.bonree.brfs.duplication.datastream.dataengine.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bonree.brfs.common.utils.PooledThreadFactory;
import com.bonree.brfs.duplication.datastream.dataengine.DataEngine;
import com.bonree.brfs.duplication.datastream.dataengine.DataStoreCallback;
import com.bonree.brfs.duplication.datastream.file.FileObject;
import com.bonree.brfs.duplication.datastream.file.FileObjectSupplier;
import com.bonree.brfs.duplication.datastream.writer.DiskWriter;
import com.bonree.brfs.duplication.datastream.writer.DiskWriter.WriteProgressListener;
import com.bonree.brfs.duplication.storageregion.StorageRegion;

public class DefaultDataEngine implements DataEngine {
	private static final Logger LOG = LoggerFactory.getLogger(DefaultDataEngine.class);
	
	private DataPool dataPool;
	private FileObjectSupplier fileSupplier;
	private DiskWriter diskWriter;
	
	private ExecutorService mainThread;
	
	private final StorageRegion storageRegion;
	
	private final AtomicBoolean runningState = new AtomicBoolean(false);
	private volatile boolean quit = false;
	
	public DefaultDataEngine(StorageRegion storageRegion, DataPool pool, FileObjectSupplier fileSupplier, DiskWriter writer) {
		this.storageRegion = storageRegion;
		this.dataPool = pool;
		this.fileSupplier = fileSupplier;
		this.diskWriter = writer;
		this.mainThread = new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<Runnable>(),
                new PooledThreadFactory("dataengine_" + storageRegion.getName()));
		
		this.mainThread.execute(new DataProcessor());
	}
	
	@Override
	public StorageRegion getStorageRegion() {
		return storageRegion;
	}
	
	@Override
	public void store(byte[] data, DataStoreCallback callback) {
		try {
			dataPool.put(new DataObject() {
				
				@Override
				public int length() {
					return data.length;
				}
				
				@Override
				public byte[] getBytes() {
					return data;
				}
				
				@Override
				public void processComplete(String result) {
					callback.dataStored(result);
				}
			});
		} catch (InterruptedException e) {
			LOG.error("store data failed", e);
			callback.dataStored(null);
		}
	}
	
	@Override
	public void close() throws IOException {
		quit = true;
		mainThread.shutdownNow();
	}
	
	private class DataProcessor implements Runnable {

		@Override
		public void run() {
			if(!runningState.compareAndSet(false, true)) {
				LOG.error("can not execute data engine again, because it's started!", new IllegalStateException("Data engine has been started!"));
				return;
			}
			
			DataObject unhandledData = null;
			
			while(true) {
				if(quit && dataPool.isEmpty()) {
					break;
				}
				
				try {
					DataObject data = unhandledData == null ? (unhandledData = dataPool.take()) : unhandledData;
					
					FileObject file = fileSupplier.fetch(data.length());
					if(file == null) {
						data.processComplete(null);
						continue;
					}
					
					List<DataObject> dataList = new ArrayList<DataObject>();
					dataList.add(data);
					unhandledData = null;
					
					while(true) {
						data = dataPool.peek();
						if(data == null) {
							break;
						}
						
						if(!file.apply(data.length())) {
							break;
						}
						
						dataList.add(data);
						dataPool.remove();
					}
					
					diskWriter.write(file, dataList, new WriteProgressListener() {
						
						@Override
						public void writeCompleted(FileObject file, boolean errorOccurred) {
							fileSupplier.recycle(file, errorOccurred);
						}
					});
				} catch (InterruptedException e) {
					LOG.error("data consumer interrupted.");
				} catch (Exception e) {
					LOG.error("process data error", e);
				}
			}
			
			LOG.info("data engine[region={}] is shut down!", storageRegion.getName());
			try {
				fileSupplier.close();
			} catch (IOException e) {
				LOG.error("close file supplier of data engine[region={}] error.", storageRegion.getName());
			}
		}
		
	}
	
	
}
