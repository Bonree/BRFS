package com.bonree.brfs.disknode.data.write;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bonree.brfs.common.timer.WheelTimer;
import com.bonree.brfs.common.timer.WheelTimer.Timeout;
import com.bonree.brfs.common.utils.CloseUtils;
import com.bonree.brfs.common.utils.LifeCycle;
import com.bonree.brfs.common.utils.TimeUtils;
import com.bonree.brfs.disknode.DiskContext;
import com.bonree.brfs.disknode.data.write.buf.ByteFileBuffer;
import com.bonree.brfs.disknode.data.write.record.RecordCollectionManager;
import com.bonree.brfs.disknode.data.write.record.RecordFileBuilder;
import com.bonree.brfs.disknode.data.write.worker.RandomWriteWorkerSelector;
import com.bonree.brfs.disknode.data.write.worker.WriteTask;
import com.bonree.brfs.disknode.data.write.worker.WriteWorker;
import com.bonree.brfs.disknode.data.write.worker.WriteWorkerGroup;
import com.bonree.brfs.disknode.data.write.worker.WriteWorkerSelector;
import com.bonree.brfs.disknode.utils.Pair;

public class FileWriterManager implements LifeCycle {
	private static final Logger LOG = LoggerFactory
			.getLogger(FileWriterManager.class);

	// 默认的写Worker线程数量
	private static final int DEFAULT_WORKER_NUMBER = Runtime.getRuntime()
			.availableProcessors();
	private WriteWorkerGroup workerGroup;
	private WriteWorkerSelector workerSelector;
	private RecordCollectionManager recorderManager;
	
	private DiskContext context;

	private static int DEFAULT_RECORD_BUFFER_SIZE = 512 * 1024;
	private static int DEFAULT_FILE_BUFFER_SIZE = 1024 * 1024;

	private Map<String, Pair<RecordFileWriter, WriteWorker>> runningWriters = new HashMap<String, Pair<RecordFileWriter, WriteWorker>>();

	private static final int DEFAULT_TIMEOUT_SECONDS = 5;
	private WheelTimer<Pair<RecordFileWriter, WriteWorker>> timeoutWheel = new WheelTimer<Pair<RecordFileWriter, WriteWorker>>(
			DEFAULT_TIMEOUT_SECONDS);

	public FileWriterManager(RecordCollectionManager recorderManager, DiskContext context) {
		this(DEFAULT_WORKER_NUMBER, recorderManager, context);
	}

	public FileWriterManager(int workerNum,
			RecordCollectionManager recorderManager, DiskContext context) {
		this(workerNum, new RandomWriteWorkerSelector(), recorderManager, context);
	}

	public FileWriterManager(int workerNum, WriteWorkerSelector selector,
			RecordCollectionManager recorderManager, DiskContext context) {
		this.workerGroup = new WriteWorkerGroup(workerNum);
		this.workerSelector = selector;
		this.recorderManager = recorderManager;
		this.context = context;
	}

	@Override
	public void start() throws Exception {
		workerGroup.start();

		timeoutWheel
				.setTimeout(new Timeout<Pair<RecordFileWriter, WriteWorker>>() {

					@Override
					public void timeout(
							Pair<RecordFileWriter, WriteWorker> target) {
						LOG.debug("timeout---{}", target.first().getPath());

						target.second().put(new WriteTask<Void>() {

							@Override
							protected Void execute() throws Exception {
								target.first().flush();
								return null;
							}

							@Override
							protected void onPostExecute(Void result) {
							}

							@Override
							protected void onFailed(Throwable e) {
								LOG.error("flush error {}", target.first()
										.getPath(), e);
							}
						});
						
						if(runningWriters.containsKey(target.first().getPath())) {
							timeoutWheel.update(target);
						}
					}
				});
		timeoutWheel.start();
		
		rebuildFileWriters();
	}
	
	private void rebuildFileWriters() throws IOException {
		File root = new File(context.getRootDir());
		
		File[] snDirList = root.listFiles();
		for(File snDir : snDirList) {
			for(File serverDir : snDir.listFiles()) {
				File[] timeDirList = serverDir.listFiles(new FilenameFilter() {
					
					@Override
					public boolean accept(File dir, String name) {
						long now = System.currentTimeMillis();
						long interval = 60 * 60 * 1000;
						
						//返回当前时间段和上一个时间段的文件
						return name.equals(TimeUtils.timeInterval(now, interval))
								|| name.equals(TimeUtils.timeInterval(now  - interval, interval));
					}
				});
				
				for(File timeDir : timeDirList) {
					File[] recordFileList = timeDir.listFiles(new FilenameFilter() {
						
						@Override
						public boolean accept(File dir, String name) {
							return RecordFileBuilder.isRecordFile(name);
						}
					});
					
					for(File recordFile : recordFileList) {
						File dataFile = RecordFileBuilder.reverse(recordFile);
						LOG.info("reopen file [{}]", dataFile);
						
						RecordFileWriter writer = new RecordFileWriter(
								recorderManager.getRecordCollection(dataFile, true, DEFAULT_RECORD_BUFFER_SIZE, true),
										new BufferedFileWriter(dataFile, true, new ByteFileBuffer(
												DEFAULT_FILE_BUFFER_SIZE)));

						Pair<RecordFileWriter, WriteWorker> binding = new Pair<RecordFileWriter, WriteWorker>(
								writer, workerSelector.select(workerGroup.getWorkerList()));
						
						runningWriters.put(dataFile.getAbsolutePath(), binding);
						timeoutWheel.update(binding);
					}
				}
			}
		}
	}

	@Override
	public void stop() {
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
								recorderManager.getRecordCollection(filePath, false,
										DEFAULT_RECORD_BUFFER_SIZE, true),
								new BufferedFileWriter(filePath,
										new ByteFileBuffer(
												DEFAULT_FILE_BUFFER_SIZE)));

						binding = new Pair<RecordFileWriter, WriteWorker>(
								writer, workerSelector.select(workerGroup
										.getWorkerList()));
						
						timeoutWheel.update(binding);
						runningWriters.put(filePath, binding);
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
			}
		}

		return binding;
	}

	public void close(String path) {
		Pair<RecordFileWriter, WriteWorker> binding = runningWriters.remove(path);
		timeoutWheel.remove(binding);
		CloseUtils.closeQuietly(binding.first());
	}
}
