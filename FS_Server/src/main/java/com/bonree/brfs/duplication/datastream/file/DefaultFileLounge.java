package com.bonree.brfs.duplication.datastream.file;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.SortedSet;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bonree.brfs.common.service.Service;
import com.bonree.brfs.common.utils.ThreadPoolUtil;
import com.bonree.brfs.duplication.DuplicationNodeSelector;
import com.bonree.brfs.duplication.coordinator.DuplicateNode;
import com.bonree.brfs.duplication.coordinator.FileCoordinator;
import com.bonree.brfs.duplication.coordinator.FileNameBuilder;
import com.bonree.brfs.duplication.coordinator.FileNode;
import com.bonree.brfs.duplication.coordinator.FileNodeSink;
import com.bonree.brfs.duplication.datastream.file.TimedObjectCollection.ObjectBuilder;
import com.bonree.brfs.duplication.datastream.file.TimedObjectCollection.TimedObject;
import com.bonree.brfs.duplication.recovery.FileRecovery;
import com.bonree.brfs.duplication.recovery.FileRecoveryListener;
import com.bonree.brfs.duplication.storagename.StorageNameManager;
import com.bonree.brfs.duplication.storagename.StorageNameNode;
import com.bonree.brfs.duplication.storagename.exception.StorageNameNonexistentException;
import com.bonree.brfs.server.identification.ServerIDManager;
import com.google.common.collect.Multimaps;
import com.google.common.collect.SortedSetMultimap;
import com.google.common.collect.TreeMultimap;

/**
 * 
 * 类的不变性约束：
 * 1、每个时间段都有一个专属的fileContainer;
 * 2、FileContainer中保存的每个元素都有一个对应的时间戳；
 * 3、FileContainer中只能保存时间戳在container对应的时间段内的元素
 * 
 * @author chen
 *
 */
public class DefaultFileLounge implements FileLounge {
	private static final Logger LOG = LoggerFactory.getLogger(DefaultFileLounge.class);
	
	//对文件节点进行清理的集合大小阈值
	private static final int FILE_SET_SIZE_CLEAN_THRESHOLD = 7;
	private static final double FILE_USAGE_RATIO_THRESHOLD = 0.99;
	private TimedObjectCollection<SortedSetMultimap<String, FileLimiter>> timedFileContainer;

	private StorageNameManager storageNameManager;
	private FileCoordinator fileCoordinator;
	private DuplicationNodeSelector duplicationSelector;
	private ServerIDManager idManager;
	
	private FileCloseListener fileCloseListener;
	
	private FileRecovery fileRecovery;
	
	private static final long DEFAULT_FILE_PATITION_TIME_INTERVAL = TimeUnit.HOURS.toMillis(1);
	//默认文件的时间分区间隔为一个小时
	private final long patitionTimeInterval;
	
	private static final long DEFAULT_CLEAN_FREQUENCY_MILLIS = TimeUnit.MINUTES.toMillis(1);
	private final long cleanFrequencyMillis;
	
	private final Service service;
	
	public DefaultFileLounge(Service service,
			StorageNameManager storageNameManager,
			FileCoordinator fileCoordinator,
			FileRecovery fileRecovery,
			DuplicationNodeSelector selector,
			ServerIDManager idManager) {
		this(service, storageNameManager, fileCoordinator, fileRecovery, selector, idManager, DEFAULT_FILE_PATITION_TIME_INTERVAL, DEFAULT_CLEAN_FREQUENCY_MILLIS);
	}
	
	public DefaultFileLounge(Service service,
			StorageNameManager storageNameManager,
			FileCoordinator fileCoordinator,
			FileRecovery fileRecovery,
			DuplicationNodeSelector selector,
			ServerIDManager idManager,
			long timeIntervalMillis,
			long cleanIntervalMillis) {
		this.service = service;
		this.storageNameManager = storageNameManager;
		this.fileCoordinator = fileCoordinator;
		this.fileRecovery = fileRecovery;
		this.duplicationSelector = selector;
		this.idManager = idManager;
		this.patitionTimeInterval = timeIntervalMillis;
		this.cleanFrequencyMillis = cleanIntervalMillis;
		
		this.timedFileContainer = new TimedObjectCollection<SortedSetMultimap<String,FileLimiter>>(
				patitionTimeInterval, TimeUnit.MILLISECONDS, new SortedSetMultimapBuilder());
		
		try {
			this.fileCoordinator.addFileNodeSink(new FileNodeShelter());
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		ThreadPoolUtil.scheduleAtFixedRate(new FileCleaner(), 0, cleanFrequencyMillis, TimeUnit.MILLISECONDS);
	}
	
	private class SortedSetMultimapBuilder implements ObjectBuilder<SortedSetMultimap<String,FileLimiter>> {

		@Override
		public SortedSetMultimap<String, FileLimiter> build() {
			return Multimaps.synchronizedSortedSetMultimap(TreeMultimap.create(new Comparator<String>() {

				@Override
				public int compare(String key1, String key2) {
					return key1.compareTo(key2);
				}
			}, new Comparator<FileLimiter>() {

				@Override
				public int compare(FileLimiter file1, FileLimiter file2) {
					//按照文件大小进行降序排列
					int sizeOrder = file2.size() - file1.size();
					return sizeOrder != 0 ? sizeOrder : file2.getFileNode().getName().compareTo(file1.getFileNode().getName());
				}
			}));
		}
		
	}
	
	@Override
	public void addFileLimiter(FileLimiter file) {
		timedFileContainer.get(file.getFileNode().getCreateTime())
		                  .put(file.getFileNode().getStorageName(), file);
	}
	
	//判断文件列表中的文件数量是否超过了设置的阈值，如果超过阈值，则对文件清理
	//清理规则：文件使用率达到指定的阈值即可清理
	private void cleanIfNeeded(Set<FileLimiter> fileLimiters) {
		if(fileLimiters.size() > FILE_SET_SIZE_CLEAN_THRESHOLD) {
			LOG.debug("Clean files because of file set size[{}]", fileLimiters.size());
			
			forEach(fileLimiters, new FileLimiterHandler() {
				
				@Override
				public boolean handle(FileLimiter file) {
					//只对文件做清理标记，文件清理程序会自动删除有清理标记的文件；如果此文件当前有写入操作的情况下
					//做清理标记会失败，没关系，可以下次在做标记
					if(file.cleanIfOverSize(FILE_USAGE_RATIO_THRESHOLD)) {
						LOG.info("Oversize clean --[{}]", file.getFileNode().getName());
					}
					
					return false;
				}

				@Override
				public boolean remove(FileLimiter file) {
					return false;
				}
				
			});
		}
	}
	
	private FileLimiter selectFileLimiter(SortedSet<FileLimiter> fileSet, int size) {
		FileLimiterSizeObtainer handler = new FileLimiterSizeObtainer(size);
		forEach(fileSet, handler);
		
		return handler.getSelected();
	}
	
	private class FileLimiterSizeObtainer implements FileLimiterHandler {
		private FileLimiter selected;
		private int size;
		
		public FileLimiterSizeObtainer(int size) {
			this.size = size;
		}
		
		public FileLimiter getSelected() {
			return selected;
		}

		@Override
		public boolean handle(FileLimiter file) {
			if(file.obtain(size)) {
				selected = file;
				return true;
			}
			
			return false;
		}

		@Override
		public boolean remove(FileLimiter file) {
			return file == selected;
		}
		
	}
	
	private interface FileLimiterHandler {
		boolean handle(FileLimiter file);
		boolean remove(FileLimiter file);
	}
	
	private void forEach(Collection<FileLimiter> fileSet, FileLimiterHandler handler) {
		Iterator<FileLimiter> iterator = fileSet.iterator();
		while(iterator.hasNext()) {
			FileLimiter file = iterator.next();
			
			boolean handled = handler.handle(file);
			
			if(handler.remove(file)) {
				iterator.remove();
			}
			
			if(handled) {
				break;
			}
		}
	}

	@Override
	public FileLimiter getFileLimiter(int storageNameId, int size) throws Exception {
		if(size > FileLimiter.DEFAULT_FILE_CAPACITY) {
			throw new DataSizeOverFlowException(size, FileLimiter.DEFAULT_FILE_CAPACITY);
		}
		
		StorageNameNode storageNameNode = storageNameManager.findStorageName(storageNameId);
		if(storageNameNode == null) {
			throw new StorageNameNonexistentException(storageNameId);
		}
		
		long currentTime = System.currentTimeMillis();
		//根据当前时间间隔获取对应的文件集合类
		SortedSetMultimap<String, FileLimiter> fileContainer = timedFileContainer.get(currentTime);
		
		synchronized (fileContainer) {
			FileLimiter file = selectFileLimiter(fileContainer.get(storageNameNode.getName()), size);
			if(file != null) {
				fileContainer.put(storageNameNode.getName(), file);
				return file;
			}
		}
		
		//没有符合条件的文件供写入，此时需要新建文件，但建立文件之前可能需要进行一些
		//清理操作
		synchronized (fileContainer) {
			cleanIfNeeded(fileContainer.get(storageNameNode.getName()));
		}
		
		DuplicateNode[] duplicateNodes = duplicationSelector.getDuplicationNodes(storageNameNode.getReplicateCount());
		FileNode fileNode = new FileNode(currentTime);
		fileNode.setName(FileNameBuilder.createFile(idManager, storageNameId, duplicateNodes));
		fileNode.setStorageName(storageNameNode.getName());
		fileNode.setStorageId(storageNameId);
		fileNode.setServiceId(service.getServiceId());
		fileNode.setDuplicateNodes(duplicateNodes);
		fileCoordinator.store(fileNode);
		
		FileLimiter fileLimiter = new FileLimiter(fileNode);
		
		fileLimiter.obtain(size);
		
		//不直接使用上面获取fileContainer是为了防止因为FileCleaner清理导致的fileContainer为null
		timedFileContainer.get(currentTime).put(storageNameNode.getName(), fileLimiter);
		
		return fileLimiter;
	}
	
	@Override
	public List<FileLimiter> getAllFileLimiterList() {
		ArrayList<FileLimiter> fileList = new ArrayList<FileLimiter>();
		
		List<TimedObject<SortedSetMultimap<String, FileLimiter>>> timedObjects = timedFileContainer.allObjects();
		for(TimedObject<SortedSetMultimap<String, FileLimiter>> obj : timedObjects) {
			SortedSetMultimap<String, FileLimiter> fileSet = obj.getObj();
			synchronized (fileSet) {
				fileList.addAll(fileSet.values());
			}
		}
		
		return fileList;
	}

	@Override
	public boolean closeFile(FileLimiter file) {
		//只对文件做清理标记
		return file.clean();
	}

	@Override
	public void setFileCloseListener(FileCloseListener listener) {
		this.fileCloseListener = listener;
	}
	
	private class FileCleaner implements Runnable {

		@Override
		public void run() {
			List<TimedObject<SortedSetMultimap<String, FileLimiter>>> timedObjects = timedFileContainer.allObjects();
			long currentTimeInterval = timedFileContainer.getTimeInterval(System.currentTimeMillis());
			
			for(TimedObject<SortedSetMultimap<String, FileLimiter>> obj : timedObjects) {
				LOG.info("{} FILE CLEANER---- {} >>> {}",new Date(), obj.getTimeInterval(), obj.getObj().size());
				SortedSetMultimap<String, FileLimiter> fileSet = obj.getObj();
				
				if(obj.getTimeInterval() < currentTimeInterval) {
					//历史时刻文件，清理所有能清理的文件
					synchronized (fileSet) {
						forEach(fileSet.values(), new FileLimiterHandler() {
							
							
							@Override
							public boolean handle(FileLimiter file) {
								return false;
							}

							@Override
							public boolean remove(FileLimiter file) {
								//向文件做清理标记
								if(file.clean()) {
									System.out.println("!!!!history close---" + file.getFileNode().getName());
									if(closeFile(file)) {
										return true;
									}
								}
								
								return false;
							}
						});
					}
					
					if(fileSet.isEmpty()) {
						timedFileContainer.remove(obj.getTimeInterval());
					}
				} else {
					//当前时刻的文件集合，只对有clean标记的文件做处理
					synchronized (fileSet) {
						forEach(fileSet.values(), new FileLimiterHandler() {
							
							@Override
							public boolean handle(FileLimiter file) {
								System.out.println("%%%CURRENT--" + file.getFileNode().getName());
								return false;
							}

							@Override
							public boolean remove(FileLimiter file) {
								//判断文件是否有清理标记
								if(file.isCleaned()) {
									System.out.println("!!!!current close---" + file.getFileNode().getName());
									if(closeFile(file)) {
										return true;
									}
								}
								
								return false;
							}
							
						});
					}
				}
			}
		}
		
		private boolean closeFile(FileLimiter file) {
			try {
				fileCoordinator.delete(file.getFileNode());
				
				if(fileCloseListener != null) {
					fileCloseListener.close(file);
				}
				
				return true;
			} catch (Exception e) {
				e.printStackTrace();
			}
			
			return false;
		}
	}
	
	private class FileNodeShelter implements FileNodeSink {

		@Override
		public Service getService() {
			return service;
		}

		@Override
		public void fill(FileNode fileNode) {
			fileRecovery.recover(fileNode, new FileRecoveryListener() {
				
				@Override
				public void complete(FileNode fileNode) {
					//TODO 同步不同副本之间的文件内容，然后获取正确的文件大小和文件序列号
					FileLimiter file = new FileLimiter(fileNode, 0/*文件大小*/, 0/*文件序列号*/);
					addFileLimiter(file);
				}

				@Override
				public void error(Throwable cause) {
					//TODO unhandled
					cause.printStackTrace();
				}
			});
		}
		
	}
}
