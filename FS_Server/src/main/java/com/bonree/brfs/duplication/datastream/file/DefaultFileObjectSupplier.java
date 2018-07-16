package com.bonree.brfs.duplication.datastream.file;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;

import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bonree.brfs.common.collection.SortedList;
import com.bonree.brfs.common.timer.TimeExchangeEventEmitter;
import com.bonree.brfs.common.timer.TimeExchangeListener;
import com.bonree.brfs.configuration.Configs;
import com.bonree.brfs.configuration.units.DuplicateNodeConfigs;
import com.bonree.brfs.duplication.datastream.file.sync.FileObjectSynchronizeCallback;
import com.bonree.brfs.duplication.datastream.file.sync.FileObjectSynchronizer;
import com.bonree.brfs.duplication.filenode.FileNode;
import com.bonree.brfs.duplication.filenode.FileNodeSink;
import com.bonree.brfs.duplication.filenode.FileNodeSinkManager;
import com.bonree.brfs.duplication.filenode.FileNodeSinkManager.StateListener;
import com.bonree.brfs.duplication.storageregion.StorageRegion;

public class DefaultFileObjectSupplier implements FileObjectSupplier, TimeExchangeListener, FileNodeSink {
	private static Logger LOG = LoggerFactory.getLogger(DefaultFileObjectSupplier.class);
	
	private FileObjectFactory fileFactory;
	
	private ExecutorService mainThread = Executors.newSingleThreadExecutor();
	
	private final int cleanLimit;
	private final int forceCleanLimit;
	private final double cleanFileLengthRatio;
	
	private volatile long fileTimeoutClock;
	
	private SortedList<FileObject> idleFileList = new SortedList<FileObject>(FileObject.LENGTH_COMPARATOR);
	private SortedList<FileObject> busyFileList = new SortedList<FileObject>(FileObject.LENGTH_COMPARATOR);
	
	private LinkedBlockingQueue<FileObject> recycleList = new LinkedBlockingQueue<FileObject>();
	
	private FileObjectCloser fileCloser;
	private FileObjectSynchronizer fileSynchronizer;
	private FileNodeSinkManager fileNodeSinkManager;
	
	private TimeExchangeEventEmitter timeEventEmitter;
	private Duration timeDuration;
	
	private final StorageRegion storageRegion;
	
	public DefaultFileObjectSupplier(StorageRegion storageRegion,
			FileObjectFactory factory,
			FileObjectCloser closer,
			FileObjectSynchronizer fileSynchronizer,
			FileNodeSinkManager fileNodeSinkManager,
			TimeExchangeEventEmitter timeEventEmitter) {
		this(storageRegion, factory, closer, fileSynchronizer, fileNodeSinkManager, timeEventEmitter,
				Configs.getConfiguration().GetConfig(DuplicateNodeConfigs.CONFIG_FILE_CLEAN_COUNT),
				Configs.getConfiguration().GetConfig(DuplicateNodeConfigs.CONFIG_MAX_FILE_COUNT),
				Configs.getConfiguration().GetConfig(DuplicateNodeConfigs.CONFIG_FILE_CLEAN_USAGE_RATE));
	}
	
	public DefaultFileObjectSupplier(StorageRegion storageRegion,
			FileObjectFactory factory,
			FileObjectCloser closer,
			FileObjectSynchronizer fileSynchronizer,
			FileNodeSinkManager fileNodeSinkManager,
			TimeExchangeEventEmitter timeEventEmitter,
			int cleanLimit,
			int forceCleanLimit,
			double cleanFileLengthRatio) {
		this.storageRegion = storageRegion;
		this.fileFactory = factory;
		this.fileCloser = closer;
		this.fileSynchronizer = fileSynchronizer;
		this.fileNodeSinkManager = fileNodeSinkManager;
		this.timeEventEmitter = timeEventEmitter;
		this.cleanLimit = Math.min(cleanLimit, forceCleanLimit);
		this.forceCleanLimit = forceCleanLimit;
		this.cleanFileLengthRatio = cleanFileLengthRatio;
		
		updateTimeEventListener(Duration.parse(storageRegion.getFilePartitionDuration()));
		this.fileNodeSinkManager.registerFileNodeSink(this);
		this.fileNodeSinkManager.addStateListener(stateListener);
	}
	
	private void updateTimeEventListener(Duration duration) {
		timeDuration = duration;
		fileTimeoutClock = timeEventEmitter.getStartTime(duration);
		timeEventEmitter.addListener(duration, this);
	}
	
	private int totalSize() {
		return idleFileList.size() + busyFileList.size();
	}
	
	@Override
	public FileObject fetch(int size) throws InterruptedException {
		Future<FileObject> future = mainThread.submit(new FileFetcher(size));
		
		try {
			return future.get();
		} catch (ExecutionException e) {
			LOG.error("fetch file failed", e);
			return null;
		}
	}
	
	@Override
	public void recycle(FileObject file, boolean needSync) {
		if(mainThread.isShutdown()) {
			fileCloser.close(file);
			return;
		}
		
		if(needSync) {
			fileSynchronizer.synchronize(file, new FileObjectSynchronizeCallback() {
				
				@Override
				public void complete(FileObject file, long fileLength) {
					if(file.length() != fileLength) {
						LOG.warn("update file[{}] length from [{}] to [{}]", file.node().getName(), file.length(), fileLength);
						file.setLength(fileLength);
					}
					
					recycle(file, false);
				}
			});
			
			return;
		}
		
		if(file.node().getCreateTime() < fileTimeoutClock) {
			//过期的文件直接清理
			fileCloser.close(file);
			return;
		}
		
		recycleList.offer(file);
	}
	
	private void recycleFileList() {
		FileObject recycleFile = null;
		while((recycleFile = recycleList.poll()) != null) {
			busyFileList.remove(recycleFile);
			idleFileList.add(recycleFile);
		}
	}
	
	private void clearList() {
		recycleFileList();
		
		LOG.info("idle file size : {}, busy file size : {}", idleFileList.size(), busyFileList.size());
		for(FileObject file : idleFileList) {
			fileCloser.close(file);
		}
		
		idleFileList.clear();
		busyFileList.clear();
	}
	
	@Override
	public void close() {
		fileNodeSinkManager.removeStateListener(stateListener);
		fileNodeSinkManager.unregisterFileNodeSink(this);
		timeEventEmitter.removeListener(timeDuration, this);
		mainThread.submit(() -> clearList());
		mainThread.shutdown();
	}
	
	private class FileFetcher implements Callable<FileObject> {
		private int dataSize;
		
		public FileFetcher(int dataSize) {
			this.dataSize = dataSize;
		}
		
		private void checkSize(int size, FileObject file) {
			if(size > file.capacity()) {
				throw new IllegalStateException("data size is too large to save to file, get " + dataSize + ", but max " + file.capacity());
			}
		}

		@Override
		public FileObject call() throws Exception {
			while(true) {
				recycleFileList();
				
				Iterator<FileObject> iter = idleFileList.iterator();
				while(iter.hasNext()) {
					FileObject file = iter.next();
					if(file.apply(dataSize)) {
						iter.remove();
						busyFileList.add(file);
						return file;
					}
					
					checkSize(dataSize, file);
					
					if((totalSize() >= cleanLimit && Double.compare(file.length(), file.capacity() * cleanFileLengthRatio) >= 0)
							|| (totalSize() >= forceCleanLimit)) {
						LOG.info("force clean to file[{}]", file.node().getName());
						iter.remove();
						fileCloser.close(file);
					}
				}
				
				List<FileObject> usableBusyFileList = new ArrayList<FileObject>();
				for(FileObject file : busyFileList) {
					if(file.free() >= dataSize) {
						usableBusyFileList.add(file);
						continue;
					}
					
					checkSize(dataSize, file);
				}
				
				LOG.info("file total size => " + totalSize());
				if(totalSize() < cleanLimit || (totalSize() < forceCleanLimit && usableBusyFileList.isEmpty())) {
					FileObject file = fileFactory.createFile(storageRegion);
					if(file == null) {
						throw new RuntimeException("can not create file node!");
					}
					
					LOG.info("create file object[{}] with capactiy[{}]", file.node().getName(), file.capacity());
					checkSize(dataSize, file);
					
					file.apply(dataSize);
					busyFileList.add(file);
					
					return file;
				}
				
				FileObject file = recycleList.take();
				recycleList.offer(file);
			}
		}
		
	}

	@Override
	public void timeExchanged(long startTime, Duration duration) {
		mainThread.submit(new Runnable() {
			
			@Override
			public void run() {
				LOG.info("Time[{}] to clear file list", new DateTime());
				
				//更新文件过期时间
				fileTimeoutClock = startTime;
				
				clearList();
				
				Duration storageRegionDuration = Duration.parse(storageRegion.getFilePartitionDuration());
				if(!storageRegionDuration.equals(duration)
						&& timeEventEmitter.removeListener(duration, DefaultFileObjectSupplier.this)) {
					updateTimeEventListener(storageRegionDuration);
				}
			}
		});
	}
	
	@Override
	public StorageRegion getStorageRegion() {
		return storageRegion;
	}

	@Override
	public void received(FileNode fileNode) {
		recycle(new FileObject(fileNode), true);
	}
	
	private FileNodeSinkManager.StateListener stateListener = new StateListener() {
		
		@Override
		public void stateChanged(boolean enable) {
			if(!enable) {
				mainThread.submit(new Runnable() {
					
					@Override
					public void run() {
						idleFileList.clear();
						
						while(!busyFileList.isEmpty()) {
							try {
								FileObject file = recycleList.take();
								
								busyFileList.remove(file);
							} catch (InterruptedException e) {
								LOG.error("recycle to remove busy file node error!", e);
							}
						}
					}
				});
			}
		}
	};
}
