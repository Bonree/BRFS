package com.bonree.brfs.duplication.datastream.file;

import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bonree.brfs.common.utils.ThreadPoolUtil;
import com.bonree.brfs.duplication.DuplicationEnvironment;
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
	private TimedObjectCollection<List<FileLimiter>> timedFileContainer;

	private FileCloseListener fileCloseListener;
	
	private FileLimiterFactory fileLimiterFactory;
	
	private static final long DEFAULT_FILE_PATITION_TIME_INTERVAL = TimeUnit.HOURS.toMillis(1);
	//默认文件的时间分区间隔为一个小时
	private final long patitionTimeInterval;
	
	private static final long DEFAULT_CLEAN_FREQUENCY_MILLIS = TimeUnit.MINUTES.toMillis(1);
	private final long cleanFrequencyMillis;
	
	private int storageId;
	
	public DefaultFileLounge(int storageId, FileLimiterFactory fileLimiterFactory) {
		this(storageId, fileLimiterFactory, DEFAULT_FILE_PATITION_TIME_INTERVAL, DEFAULT_CLEAN_FREQUENCY_MILLIS);
	}
	
	public DefaultFileLounge(int storageId, FileLimiterFactory fileLimiterFactory, long timeIntervalMillis, long cleanIntervalMillis) {
		this.storageId = storageId;
		this.fileLimiterFactory = fileLimiterFactory;
		this.patitionTimeInterval = timeIntervalMillis;
		this.cleanFrequencyMillis = cleanIntervalMillis;
		
		this.timedFileContainer = new TimedObjectCollection<List<FileLimiter>>(
				patitionTimeInterval, TimeUnit.MILLISECONDS, new ObjectBuilder<List<FileLimiter>>() {

					@Override
					public List<FileLimiter> build() {
						return new LinkedList<FileLimiter>();
					}
					
				});
		
		ThreadPoolUtil.scheduleAtFixedRate(new FileCleaner(), 0, cleanFrequencyMillis, TimeUnit.MILLISECONDS);
	}
	
	@Override
	public void addFileLimiter(FileLimiter file) {
		List<FileLimiter> fileList = timedFileContainer.get(file.getFileNode().getCreateTime());
		synchronized (fileList) {
			fileList.add(file);
		}
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
		List<FileLimiter> fileList = timedFileContainer.get(currentTime);
		for(int i = 0; i < requestSizes.length; i++) {
			if(requestSizes[i] > DuplicationEnvironment.DEFAULT_MAX_FILE_SIZE) {
				LOG.error("####request size is too big--{}", requestSizes[i]);
				results[i] = null;
				continue;
			}
			
			synchronized (fileList) {
				for(FileLimiter file : fileList) {
					if(!file.lock(requestSizes)) {
//						LOG.info("can not lock file[{}]", file.getFileNode().getName());
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
				
				LOG.info("create new FileLimiter--->{}", newFile);
				newFile.lock(requestSizes);
				newFile.apply(requestSizes[i]);
				results[i] = newFile;
				
				//不直接使用上面获取fileContainer是为了防止因为FileCleaner清理导致的fileContainer为null
				List<FileLimiter> tempFileList = timedFileContainer.get(currentTime);
				synchronized (tempFileList) {
					tempFileList.add(newFile);
				}
			}
		}
		
		return results;
	}
	
	@Override
	public void clear() {
		
	}

	@Override
	public void setFileCloseListener(FileCloseListener listener) {
		this.fileCloseListener = listener;
	}
	
	private class FileCleaner implements Runnable {
		private LinkedList<FileLimiter> removedFileList = new LinkedList<FileLimiter>();

		@Override
		public void run() {
			List<TimedObject<List<FileLimiter>>> timedObjects = timedFileContainer.allObjects();
			long currentTimeInterval = timedFileContainer.getTimeInterval(System.currentTimeMillis());
			
			for(TimedObject<List<FileLimiter>> obj : timedObjects) {
				LOG.info("{} FILE CLEANER---- {} >>> {}",new Date(), obj.getTimeInterval(), obj.getObj().size());
				List<FileLimiter> fileList = obj.getObj();
				
				if(obj.getTimeInterval() < currentTimeInterval) {
					//历史时刻文件，清理所有能清理的文件
					synchronized (fileList) {
						Iterator<FileLimiter> iterator = fileList.iterator();
						while(iterator.hasNext()) {
							FileLimiter file = iterator.next();
							if(file.lock(this)) {
								System.out.println("!!!!history close---" + file.getFileNode().getName());
								removedFileList.add(file);
								iterator.remove();
							}
						}
					}
					
					if(fileList.isEmpty()) {
						timedFileContainer.remove(obj.getTimeInterval());
					}
				} else {
					//当前时刻的文件集合，只对有clean标记的文件做处理
					synchronized (fileList) {
						boolean cleanOverSize = fileList.size() > FILE_SET_SIZE_CLEAN_THRESHOLD;
						Iterator<FileLimiter> iterator = fileList.iterator();
						while(iterator.hasNext()) {
							FileLimiter file = iterator.next();
							
							if(cleanOverSize
									&& Double.compare(file.getLength(), file.capacity() * FILE_USAGE_RATIO_THRESHOLD) > 0
									&& file.lock(this)) {
								System.out.println("!!!!current close---" + file.getFileNode().getName());
								removedFileList.add(file);
								iterator.remove();
							}
						}
					}
				}
			}
			
			if(fileCloseListener == null) {
				removedFileList.clear();
				return;
			}
			
			Iterator<FileLimiter> iterator = removedFileList.iterator();
			while(iterator.hasNext()) {
				try {
					fileCloseListener.close(iterator.next());
					
					iterator.remove();
				} catch (Exception e) {
				}
			}
		}
	}
}
