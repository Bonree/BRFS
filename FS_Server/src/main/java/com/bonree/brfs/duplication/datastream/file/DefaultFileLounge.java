package com.bonree.brfs.duplication.datastream.file;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bonree.brfs.common.timer.TimeCounter;
import com.bonree.brfs.duplication.DuplicationEnvironment;
import com.bonree.brfs.duplication.coordinator.DuplicateNode;
import com.bonree.brfs.duplication.coordinator.FileNode;
import com.bonree.brfs.duplication.coordinator.FilePathBuilder;
import com.bonree.brfs.duplication.datastream.connection.DiskNodeConnection;
import com.bonree.brfs.duplication.synchronize.FileSynchronizeCallback;
import com.bonree.brfs.duplication.synchronize.FileSynchronizer;
import com.bonree.brfs.duplication.utils.TimedObjectCollection;
import com.bonree.brfs.duplication.utils.TimedObjectCollection.ObjectBuilder;
import com.bonree.brfs.duplication.utils.TimedObjectCollection.TimedObject;

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
	private static final String KEY_FILE_SET_SIZE_CLEAN = "file_clean_size";
	private static final int FILE_SET_SIZE_CLEAN_THRESHOLD = Integer.parseInt(System.getProperty(KEY_FILE_SET_SIZE_CLEAN, "3"));
	private static final String KEY_FILE_USAGE_RATIO = "file_usage_ratio";
	private static final double FILE_USAGE_RATIO_THRESHOLD = Double.parseDouble(System.getProperty(KEY_FILE_USAGE_RATIO, "0.99"));
	private TimedObjectCollection<List<FileLimiter>> timedWritableFileContainer;
	private LinkedList<FileLimiter> removedFileList = new LinkedList<FileLimiter>();
	
	private static final int DEFAULT_MAX_FILE_COUNT = 20;
	private int maxFileCount = Integer.parseInt(System.getProperty("max_file_count", String.valueOf(DEFAULT_MAX_FILE_COUNT)));
	
	private TimedObjectCollection<List<FileLimiter>> suspendFileContainer;

	private FileCloseListener fileCloseListener;
	
	private FileLimiterFactory fileLimiterFactory;
	
	private FileSynchronizer fileSynchronizer;
	private FileLimiterStateRebuilder fileRebuilder;
	
	private static final long DEFAULT_FILE_PATITION_TIME_INTERVAL = TimeUnit.HOURS.toMillis(1);
	//默认文件的时间分区间隔为一个小时
	private final long patitionTimeInterval;
	
	private int storageId;
	
	public DefaultFileLounge(int storageId, FileLimiterFactory fileLimiterFactory, FileSynchronizer fileSynchronizer, FileLimiterStateRebuilder fileRebuilder) {
		this(storageId, fileLimiterFactory, fileSynchronizer, fileRebuilder, DEFAULT_FILE_PATITION_TIME_INTERVAL);
	}
	
	public DefaultFileLounge(int storageId, FileLimiterFactory fileLimiterFactory, FileSynchronizer fileSynchronizer, FileLimiterStateRebuilder fileRebuilder, long timeIntervalMillis) {
		this.storageId = storageId;
		this.fileLimiterFactory = fileLimiterFactory;
		this.fileSynchronizer = fileSynchronizer;
		this.fileRebuilder = fileRebuilder;
		this.patitionTimeInterval = timeIntervalMillis;
		
		this.timedWritableFileContainer = new TimedObjectCollection<List<FileLimiter>>(
				patitionTimeInterval, TimeUnit.MILLISECONDS, new ObjectBuilder<List<FileLimiter>>() {

					@Override
					public List<FileLimiter> build() {
						return new LinkedList<FileLimiter>();
					}
					
				});
		
		this.suspendFileContainer = new TimedObjectCollection<List<FileLimiter>>(
				patitionTimeInterval, TimeUnit.MILLISECONDS, new ObjectBuilder<List<FileLimiter>>() {

					@Override
					public List<FileLimiter> build() {
						return new LinkedList<FileLimiter>();
					}
					
				});
	}
	
	private void addFileLimiter(List<FileLimiter> list, FileLimiter file) {
		synchronized (list) {
			list.add(file);
		}
	}
	
	@Override
	public void addFileLimiter(FileLimiter file) {
		addFileLimiter(timedWritableFileContainer.get(file.getFileNode().getCreateTime()), file);
	}

	@Override
	public FileLimiter getFileLimiter(int size) {
		return getFileLimiterList(new int[]{size})[0];
	}
	
	@Override
	public FileLimiter[] getFileLimiterList(int[] requestSizes) {
		FileLimiter[] results = new FileLimiter[requestSizes.length];
		
		long currentTime = System.currentTimeMillis();
		Set<FileLimiter> selected = new HashSet<FileLimiter>();
		List<FileLimiter> fileList = timedWritableFileContainer.get(currentTime);
		
		for(int i = 0; i < requestSizes.length; i++) {
			if(requestSizes[i] > DuplicationEnvironment.DEFAULT_MAX_AVAILABLE_FILE_SPACE) {
				LOG.error("####request size[{}] is bigger than MAX_AVAILABLE_SIZE[{}]",
						requestSizes[i], DuplicationEnvironment.DEFAULT_MAX_AVAILABLE_FILE_SPACE);
				results[i] = null;
				continue;
			}
			
			synchronized (fileList) {
				Iterator<FileLimiter> iterator = fileList.iterator();
				while(iterator.hasNext()) {
					FileLimiter file = iterator.next();
					if(file.isSync()) {
						LOG.info("skip selecting file[{}] because it's syncing", file.getFileNode().getName());
						fileSynchronizer.synchronize(file.getFileNode(), new FileLimiterSyncCallback(file));
						iterator.remove();
						
						addFileLimiter(suspendFileContainer.get(file.getFileNode().getCreateTime()), file);
						continue;
					}
					
					if(!file.lock(requestSizes)) {
						LOG.debug("can not lock file[{}]", file.getFileNode().getName());
						continue;
					}
					
					if(file.apply(requestSizes[i])) {
						results[i] = file;
						selected.add(file);
						break;
					}
					
					if(!selected.contains(file)) {
						file.unlock();
					}
				}
			}
			
			if(results[i] == null) {
				FileLimiter newFile = fileLimiterFactory.create(currentTime, storageId);
				if(newFile == null) {
					throw new RuntimeException("can not create FileLimiter???");
				}
				
				LOG.info("create new FileLimiter--->{}", newFile.getFileNode().getName());
				newFile.lock(requestSizes);
				newFile.apply(requestSizes[i]);
				results[i] = newFile;
				
				//不直接使用上面获取fileContainer是为了防止因为FileCleaner清理导致的fileContainer为null
				addFileLimiter(timedWritableFileContainer.get(currentTime), newFile);
			}
		}
		
		return results;
	}
	
	@Override
	public void clean() {
		List<TimedObject<List<FileLimiter>>> timedObjects = timedWritableFileContainer.allObjects();
		long currentTimeInterval = timedWritableFileContainer.getTimeInterval(System.currentTimeMillis());
		
		TimeCounter counter = new TimeCounter("FileClean", TimeUnit.MILLISECONDS);
		counter.begin();
		List<TimedObject<List<FileLimiter>>> syncingFiles = suspendFileContainer.allObjects();
		for(TimedObject<List<FileLimiter>> obj : syncingFiles) {
			//移除所有在同步状态的文件
			if(obj.getTimeInterval() < currentTimeInterval) {
				List<FileLimiter> fileList = obj.getObj();
				synchronized(fileList) {
					Iterator<FileLimiter> iterator = fileList.iterator();
					while(iterator.hasNext()) {
						FileLimiter file = iterator.next();
						
						LOG.info("close suspended file[{}]", file.getFileNode().getName());
						removedFileList.add(file);
						iterator.remove();
					}
				}
			}
		}
		
		LOG.info(counter.report(0));
		
		for(TimedObject<List<FileLimiter>> obj : timedObjects) {
			List<FileLimiter> fileList = obj.getObj();
			LOG.info("container[{}] FILE CLEANER---- at {} >>> size[{}]", timedWritableFileContainer, obj.getTimeInterval(), fileList.size());
			
			if(obj.getTimeInterval() < currentTimeInterval) {
				LOG.info("clean historical file list!");
				//历史时刻文件，清理所有能清理的文件
				synchronized (fileList) {
					Iterator<FileLimiter> iterator = fileList.iterator();
					while(iterator.hasNext()) {
						FileLimiter file = iterator.next();
						if(!file.lock(this)) {
							LOG.info("can not remove HISTORICAL locked file[{}]", file.getFileNode().getName());
							continue;
						}
						
						LOG.info("CLOSE historical file ---{}", file.getFileNode().getName());
						removedFileList.add(file);
						iterator.remove();
					}
				}
				
				if(fileList.isEmpty()) {
					timedWritableFileContainer.remove(obj.getTimeInterval());
				}
			} else {
				//当前时刻的文件集合，只对有clean标记的文件做处理
				synchronized (fileList) {
					boolean cleanOverSize = fileList.size() >= FILE_SET_SIZE_CLEAN_THRESHOLD;
					
					if(!cleanOverSize) {
						//文件数量没达到阈值，不进行清理
						LOG.info("file list size[{}] is smaller than threshold[{}], don't clean list.", fileList.size(),  FILE_SET_SIZE_CLEAN_THRESHOLD);
						continue;
					}
					
					Iterator<FileLimiter> iterator = fileList.iterator();
					while(iterator.hasNext()) {
						FileLimiter file = iterator.next();
						
						if(Double.compare(file.getLength(), file.capacity() * FILE_USAGE_RATIO_THRESHOLD) < 0) {
							//文件大小没达到指定阈值，不进行清理
							LOG.info("ignore current file[{}] contains [{}] bytes, not reach [{} * {}]",
									file.getFileNode().getName(),
									file.getLength(),
									file.capacity(),
									FILE_USAGE_RATIO_THRESHOLD);
							continue;
						}
						
						if(!file.lock(this)) {
							//无法锁定文件，说明当前文件还有写入操作，不进行清理
							LOG.info("can not remove CURRENT locked file[{}]", file.getFileNode().getName());
							continue;
						}
						
						LOG.info("close current file ---{}", file.getFileNode().getName());
						removedFileList.add(file);
						iterator.remove();
					}
				}
			}
		}
		
		LOG.info(counter.report(1));
		
		if(fileCloseListener == null) {
			removedFileList.clear();
			return;
		}
		
		Iterator<FileLimiter> iterator = removedFileList.iterator();
		while(iterator.hasNext()) {
			try {
				FileLimiter file = iterator.next();
				LOG.info("scaning closing file-->{}", file.getFileNode().getName());
				fileCloseListener.close(file);
				
				iterator.remove();
			} catch (Exception e) {
			}
		}
	}

	@Override
	public void setFileCloseListener(FileCloseListener listener) {
		this.fileCloseListener = listener;
	}

	@Override
	public List<FileLimiter> listFileLimiters() {
		List<FileLimiter> result = new ArrayList<FileLimiter>();
		List<TimedObject<List<FileLimiter>>> timedObjects = timedWritableFileContainer.allObjects();
		for(TimedObject<List<FileLimiter>> obj : timedObjects) {
			result.addAll(obj.getObj());
		}
		
		return result;
	}
	
	private class FileLimiterSyncCallback implements FileSynchronizeCallback {
		private FileLimiter fileLimiter;
		
		public FileLimiterSyncCallback(FileLimiter fileLimiter) {
			this.fileLimiter = fileLimiter;
		}

		@Override
		public void complete(FileNode file) {
			LOG.info("after sync, file Limiter[{}] is back!", file.getName());
			long currentTime = timedWritableFileContainer.getTimeInterval(System.currentTimeMillis());
			long fileTime = timedWritableFileContainer.getTimeInterval(file.getCreateTime());
			if(currentTime == fileTime) {
				FileLimiter tempFile = fileRebuilder.rebuild(fileLimiter);
				if(tempFile == null) {
					LOG.error("can not rebuild file state of [{}] after sync", file.getName());
					return;
				}
				
				//只有处于当前时刻的文件才需要回归到写入列表
				fileLimiter.setSync(false);
				addFileLimiter(fileLimiter);
				
				List<FileLimiter> suspendFileList = suspendFileContainer.get(file.getCreateTime());
				synchronized (suspendFileList) {
					Iterator<FileLimiter> iterator = suspendFileList.iterator();
					while(iterator.hasNext()) {
						FileLimiter next = iterator.next();
						if(next.getFileNode().getName().equals(file.getName())) {
							iterator.remove();
							break;
						}
					}
				}
			}
		}

		@Override
		public void error(Throwable cause) {
			LOG.error("file limiter[{}] sync error", fileLimiter.getFileNode().getName(), cause);
		}
		
	}
}
