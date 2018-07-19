package com.bonree.brfs.duplication.datastream.file;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bonree.brfs.common.collection.SortedList;
import com.bonree.brfs.common.timer.TimeExchangeEventEmitter;
import com.bonree.brfs.common.timer.TimeExchangeListener;
import com.bonree.brfs.common.utils.PooledThreadFactory;
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
	
	private ExecutorService mainThread;
	
	private final int cleanLimit;
	private final int forceCleanLimit;
	private final double cleanFileLengthRatio;
	
	private SortedList<FileObject> idleFileList = new SortedList<FileObject>(FileObject.LENGTH_COMPARATOR);
	private SortedList<FileObject> busyFileList = new SortedList<FileObject>(FileObject.LENGTH_COMPARATOR);
	
	private AtomicBoolean recycleSignal = new AtomicBoolean(false);
	private List<FileObject> recycleFileList = Collections.synchronizedList(new ArrayList<FileObject>());
	private List<FileObject> exceptionFileList = Collections.synchronizedList(new ArrayList<FileObject>());
	
	private FileObjectCloser fileCloser;
	private FileObjectSynchronizer fileSynchronizer;
	private FileNodeSinkManager fileNodeSinkManager;
	
	private TimeExchangeEventEmitter timeEventEmitter;
	
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
		this.mainThread = Executors.newSingleThreadExecutor(new PooledThreadFactory(storageRegion.getName() + "_file_supplier"));
		
		this.timeEventEmitter.addListener(Duration.parse(storageRegion.getFilePartitionDuration()), this);
		this.fileNodeSinkManager.registerFileNodeSink(this);
		this.fileNodeSinkManager.addStateListener(stateListener);
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
		try {
			if(needSync) {
				exceptionFileList.add(file);
				
				fileSynchronizer.synchronize(file, new FileObjectSynchronizeCallback() {
					
					@Override
					public void complete(FileObject file, long fileLength) {
						if(file.length() != fileLength) {
							LOG.warn("update file[{}] length from [{}] to [{}]", file.node().getName(), file.length(), fileLength);
							file.setLength(fileLength);
						}
						
						exceptionFileList.remove(file);
						recycle(file, false);
					}
				});
				
				return;
			}
			
			recycleFileList.add(file);
		} finally {
			recycleSignal.set(true);
		}
	}
	
	private void recycleFileObjects() {
		synchronized (recycleFileList) {
			for(FileObject file : recycleFileList) {
				if(file.getState() == FileObject.STATE_ABANDON) {
					continue;
				}
				
				if(file.getState() == FileObject.STATE_CLOSING) {
					fileCloser.close(file);
					continue;
				}
				
				idleFileList.add(file);
				busyFileList.remove(file);
			}
		}
		
		synchronized (exceptionFileList) {
			for(FileObject file : exceptionFileList) {
				busyFileList.remove(file);
			}
		}
		
		recycleFileList.clear();
	}
	
	private void clearList() {
		recycleFileObjects();
		
		synchronized (exceptionFileList) {
			for(FileObject file : exceptionFileList) {
				file.setState(FileObject.STATE_CLOSING);
			}
		}
		
		for(FileObject file : busyFileList) {
			file.setState(FileObject.STATE_CLOSING);
		}
		busyFileList.clear();
		
		for(FileObject file : idleFileList) {
			fileCloser.close(file);
		}
		idleFileList.clear();
	}
	
	@Override
	public void close() {
		fileNodeSinkManager.removeStateListener(stateListener);
		fileNodeSinkManager.unregisterFileNodeSink(this);
		timeEventEmitter.removeListener(Duration.parse(storageRegion.getFilePartitionDuration()), this);
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
				recycleFileObjects();
				
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
				
				while(!recycleSignal.compareAndSet(true, false)) {
					Thread.yield();
				}
			}
		}
		
	}

	@Override
	public void timeExchanged(long startTime, Duration duration) {
		mainThread.submit(new Runnable() {
			
			@Override
			public void run() {
				LOG.info("Time[{}] to clear file list", new DateTime());
				clearList();
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
						
						for(FileObject file : busyFileList) {
							file.setState(FileObject.STATE_ABANDON);
						}
						
						synchronized (exceptionFileList) {
							for(FileObject file : exceptionFileList) {
								file.setState(FileObject.STATE_ABANDON);
							}
						}
						
						busyFileList.clear();
						recycleFileList.clear();
						exceptionFileList.clear();
					}
				});
			}
		}
	};
}
