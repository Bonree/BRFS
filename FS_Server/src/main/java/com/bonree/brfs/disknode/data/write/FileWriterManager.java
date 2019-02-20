package com.bonree.brfs.disknode.data.write;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bonree.brfs.common.process.LifeCycle;
import com.bonree.brfs.common.timer.WheelTimer;
import com.bonree.brfs.common.timer.WheelTimer.Timeout;
import com.bonree.brfs.common.utils.BRFSFileUtil;
import com.bonree.brfs.common.utils.BRFSPath;
import com.bonree.brfs.common.utils.ByteUtils;
import com.bonree.brfs.common.utils.CloseUtils;
import com.bonree.brfs.common.utils.FileUtils;
import com.bonree.brfs.common.write.data.FileDecoder;
import com.bonree.brfs.configuration.Configs;
import com.bonree.brfs.configuration.units.DataNodeConfigs;
import com.bonree.brfs.disknode.boot.FileValidChecker;
import com.bonree.brfs.disknode.data.read.DataFileReader;
import com.bonree.brfs.disknode.data.write.buf.ByteArrayFileBuffer;
import com.bonree.brfs.disknode.data.write.record.RecordCollectionManager;
import com.bonree.brfs.disknode.data.write.record.RecordElement;
import com.bonree.brfs.disknode.data.write.record.RecordFileBuilder;
import com.bonree.brfs.disknode.data.write.worker.RandomWriteWorkerSelector;
import com.bonree.brfs.disknode.data.write.worker.WriteTask;
import com.bonree.brfs.disknode.data.write.worker.WriteWorker;
import com.bonree.brfs.disknode.data.write.worker.WriteWorkerGroup;
import com.bonree.brfs.disknode.data.write.worker.WriteWorkerSelector;
import com.bonree.brfs.disknode.utils.BRFSRdFileFilter;
import com.bonree.brfs.disknode.utils.Pair;
import com.google.common.base.Splitter;

public class FileWriterManager implements LifeCycle {
	private static final Logger LOG = LoggerFactory.getLogger(FileWriterManager.class);

	// 默认的写Worker线程数量
	private WriteWorkerGroup workerGroup;
	private WriteWorkerSelector workerSelector;
	private RecordCollectionManager recorderManager;
	
	private FileValidChecker checker;

	private static int recordCacheSize = Configs.getConfiguration().GetConfig(DataNodeConfigs.CONFIG_WRITER_RECORD_CACHE_SIZE);
	private static int dataCacheSize = Configs.getConfiguration().GetConfig(DataNodeConfigs.CONFIG_WRITER_DATA_CACHE_SIZE);

	private ConcurrentHashMap<String, Pair<RecordFileWriter, WriteWorker>> runningWriters = new ConcurrentHashMap<String, Pair<RecordFileWriter, WriteWorker>>();

	Duration fileIdleDuration = Duration.parse(Configs.getConfiguration().GetConfig(DataNodeConfigs.CONFIG_FILE_IDLE_TIME));
	private WheelTimer<String> timeoutWheel = new WheelTimer<String>((int) fileIdleDuration.getSeconds());

	public FileWriterManager(RecordCollectionManager recorderManager, FileValidChecker checker) {
		this(Configs.getConfiguration().GetConfig(DataNodeConfigs.CONFIG_WRITER_WORKER_NUM), recorderManager, checker);
	}

	public FileWriterManager(int workerNum, RecordCollectionManager recorderManager, FileValidChecker checker) {
		this(workerNum, new RandomWriteWorkerSelector(), recorderManager, checker);
	}

	public FileWriterManager(int workerNum, WriteWorkerSelector selector,
			RecordCollectionManager recorderManager, FileValidChecker checker) {
		this.workerGroup = new WriteWorkerGroup(workerNum);
		this.workerSelector = selector;
		this.recorderManager = recorderManager;
		this.checker = checker;
	}

	@Override
	public void start() throws Exception {
		workerGroup.start();

		timeoutWheel.setTimeout(new Timeout<String>() {

					@Override
					public void timeout(String filePath) {
						LOG.info("Time to flush file[{}]", filePath);

						try {
							flushFile(filePath);
						} catch (FileNotFoundException e) {
							LOG.error("flush file[{}] error", filePath, e);
						}
					}
				});
		timeoutWheel.start();
	}
	
	public void flushIfNeeded(String filePath) {
		timeoutWheel.update(filePath);
	}
	
	public void flushFile(String path) throws FileNotFoundException {
		Pair<RecordFileWriter, WriteWorker> binding = getBinding(path, false);
		if(binding == null) {
			throw new FileNotFoundException(path);
		}
		
		binding.second().put(new WriteTask<Void>() {

			@Override
			protected Void execute() throws Exception {
				LOG.info("execute flush for file[{}] BEGIN", binding.first().getPath());
				if(runningWriters.containsKey(path)) {
					binding.first().flush();
				}
				
				return null;
			}

			@Override
			protected void onPostExecute(Void result) {
				LOG.info("execute flush for file[{}] OVER", binding.first().getPath());
			}

			@Override
			protected void onFailed(Throwable e) {
				LOG.error("flush error {}", path, e);
			}
		});
	}
	
	public void flushAll() {
		for(String filePath : runningWriters.keySet()) {
			try {
				flushFile(filePath);
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			}
		}
	}

    public void rebuildFileWriterbyDir(String dataDirPath) throws IOException {
	    Map<String,String> baseMap = new HashMap<>();
	    List<BRFSPath> rds = BRFSFileUtil.scanBRFSFiles(dataDirPath,baseMap,baseMap.size(),new BRFSRdFileFilter());
	    File rdFile = null;
	    File dataFile = null;
	    for(BRFSPath path : rds){
	        rdFile = new File(new StringBuilder().append(dataDirPath).append(FileUtils.FILE_SEPARATOR).append(path).toString());
	        dataFile = RecordFileBuilder.reverse(rdFile);
            rebuildFileWriter(dataFile);
            if(!checker.isValid(dataFile.getName())) {
                LOG.error(" file [{}] can't find in zk", dataFile.getAbsolutePath());
            }
        }

    }

	@Override
	public void stop() {
		for(Entry<String,Pair<RecordFileWriter,WriteWorker>> entry : runningWriters.entrySet()) {
			try {
				entry.getValue().first().flush();
			} catch (IOException e) {
				LOG.error("stop to flush file[{}] error", entry.getKey(), e);
			}
		}
		workerGroup.stop();
	}

	public Pair<RecordFileWriter, WriteWorker> getBinding(String path, boolean createIfNeeded) {
		Pair<RecordFileWriter, WriteWorker> binding = runningWriters.get(path);

		if (binding != null) {
			return binding;
		}

		if (!createIfNeeded) {
			return null;
		}

		return buildDiskWriter(path);
	}
	
	public void rebuildFileWriter(File dataFile) throws IOException {
		RecordFileWriter writer = new RecordFileWriter(
				recorderManager.getRecordCollection(dataFile, true, recordCacheSize, true),
						new BufferedFileWriter(dataFile, true, new ByteArrayFileBuffer(dataCacheSize)));

		Pair<RecordFileWriter, WriteWorker> binding = new Pair<RecordFileWriter, WriteWorker>(
				writer, workerSelector.select(workerGroup.getWorkerList()));
		
		runningWriters.put(dataFile.getAbsolutePath(), binding);
	}

	private Pair<RecordFileWriter, WriteWorker> buildDiskWriter(String filePath) {
		Pair<RecordFileWriter, WriteWorker> binding = runningWriters.get(filePath);

		if (binding == null) {
			synchronized (runningWriters) {
				binding = runningWriters.get(filePath);
				if (binding == null) {
					try {
						File file = new File(filePath);
						if(!file.getParentFile().exists()) {
							file.getParentFile().mkdirs();
						}
						
						RecordFileWriter writer = new RecordFileWriter(
								recorderManager.getRecordCollection(filePath, false, recordCacheSize, true),
								new BufferedFileWriter(filePath, new ByteArrayFileBuffer(dataCacheSize)));

						binding = new Pair<RecordFileWriter, WriteWorker>(
								writer, workerSelector.select(workerGroup
										.getWorkerList()));
						
						runningWriters.put(filePath, binding);
					} catch (Exception e) {
						LOG.error("build disk writer error", e);
					}
				}
			}
		}

		return binding;
	}
	
	//获取缺失或多余的日志记录信息
	private List<RecordElement> validElements(String filepath, List<RecordElement> originElements) {
		byte[] bytes = DataFileReader.readFile(filepath, 0);
		List<String> offsets = FileDecoder.getOffsets(bytes);
		
		List<RecordElement> validElmentList = new ArrayList<RecordElement>();
		RecordElement element = originElements.get(0);
		if(element.getOffset() != 0) {
			//没有文件头的日志记录，不应该发生的
			throw new IllegalStateException("no header record in file[" + filepath + "]");
		}
		
		validElmentList.add(element);
		int index = 0;
		int originSize = originElements.size();
		for(index = 0; index < offsets.size(); index++) {
			List<String> parts = Splitter.on("|").splitToList(offsets.get(index));
			int offset = Integer.parseInt(parts.get(0));
			int size = Integer.parseInt(parts.get(1));
			long crc = ByteUtils.crc(bytes, offset, size);
			
			if(index + 1 >= originSize) {
				//数据文件还有数据，但日志文件没有记录
				validElmentList.add(new RecordElement(offset, size, crc));
				continue;
			}
			
			element = originElements.get(index + 1);
			
			if(element.getOffset() != offset) {
				LOG.warn("excepted offset[{}], but get offset[{}] for file[{}]", offset, element.getOffset(), filepath);
				break;
			}
			
			if(element.getSize() != size) {
				LOG.warn("excepted size[{}], but get size[{}] for file[{}]", size, element.getSize(), filepath);
				break;
			}
			
			if(element.getCrc() != crc) {
				LOG.warn("excepted crc[{}], but get crc[{}] for file[{}]", crc, element.getCrc(), filepath);
				break;
			}
			
			validElmentList.add(element);
		}
		
		return validElmentList;
	}
	
	public void adjustFileWriter(String filePath) throws IOException {
		Pair<RecordFileWriter, WriteWorker> binding = runningWriters.get(filePath);
		if(binding == null) {
			throw new IllegalStateException("no writer of " + filePath + " is found for adjust");
		}
		
		List<RecordElement> originElements = binding.first().getRecordCollection().getRecordElementList();
		if(originElements.isEmpty()) {
			//没有数据写入成功，不需要任何协调
			return;
		}
		
		List<RecordElement> elements = validElements(filePath, originElements);
		LOG.info("adjust file get elements size[{}] for file[{}]", elements.size(), filePath);
		RecordElement lastElement = elements.get(elements.size() - 1);
		long validPosition = lastElement.getOffset() + lastElement.getSize();
		LOG.debug("last element : {}", lastElement);
		
		boolean needFlush = false;
		if(validPosition != binding.first().position()) {
			LOG.info("rewrite file content of file[{}] from[{}] to [{}]", filePath, binding.first().position(), validPosition);
			//数据文件的内容和日志信息不一致，需要调整数据文件
			binding.first().position(validPosition);
			needFlush = true;
		}
		
		if(elements.size() != originElements.size()) {
			LOG.info("rewrite file records of file[{}]", filePath);
			binding.first().getRecordCollection().clear();
			for(RecordElement element : elements) {
				binding.first().getRecordCollection().put(element);
			}
			needFlush = true;
		}
		
		if(needFlush) {
			binding.first().flush();
		}
	}

	public void close(String path) {
		Pair<RecordFileWriter, WriteWorker> binding = runningWriters.remove(path);
		if(binding == null) {
			return;
		}
		
		timeoutWheel.remove(path);
		CloseUtils.closeQuietly(binding.first());
	}
}
