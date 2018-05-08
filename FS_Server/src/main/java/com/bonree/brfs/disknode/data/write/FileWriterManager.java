package com.bonree.brfs.disknode.data.write;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bonree.brfs.common.timer.WheelTimer;
import com.bonree.brfs.common.timer.WheelTimer.Timeout;
import com.bonree.brfs.common.utils.CloseUtils;
import com.bonree.brfs.common.utils.LifeCycle;
import com.bonree.brfs.disknode.data.write.buf.ByteFileBuffer;
import com.bonree.brfs.disknode.data.write.record.RecordCollectionManager;
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

	private static int DEFAULT_RECORD_BUFFER_SIZE = 512 * 1024;
	private static int DEFAULT_FILE_BUFFER_SIZE = 1024 * 1024;

	private Map<String, Pair<RecordFileWriter, WriteWorker>> runningWriters = new HashMap<String, Pair<RecordFileWriter, WriteWorker>>();

	private static final int DEFAULT_TIMEOUT_SECONDS = 5;
	private WheelTimer<Pair<RecordFileWriter, WriteWorker>> timeoutWheel = new WheelTimer<Pair<RecordFileWriter, WriteWorker>>(
			DEFAULT_TIMEOUT_SECONDS);

	public FileWriterManager(RecordCollectionManager recorderManager) {
		this(DEFAULT_WORKER_NUMBER, recorderManager);
	}

	public FileWriterManager(int workerNum,
			RecordCollectionManager recorderManager) {
		this(workerNum, new RandomWriteWorkerSelector(), recorderManager);
	}

	public FileWriterManager(int workerNum, WriteWorkerSelector selector,
			RecordCollectionManager recorderManager) {
		this.workerGroup = new WriteWorkerGroup(workerNum);
		this.workerSelector = selector;
		this.recorderManager = recorderManager;
	}

	@Override
	public void start() {
		workerGroup.start();

		timeoutWheel
				.setTimeout(new Timeout<Pair<RecordFileWriter, WriteWorker>>() {

					@Override
					public void timeout(
							Pair<RecordFileWriter, WriteWorker> target) {
						LOG.info("timeout---{}", target.first().getPath());

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
		Pair<RecordFileWriter, WriteWorker> binding = runningWriters
				.get(filePath);

		if (binding == null) {
			synchronized (runningWriters) {
				binding = runningWriters.get(filePath);
				if (binding == null) {
					try {
						RecordFileWriter writer = new RecordFileWriter(
								recorderManager.getRecordCollection(filePath,
										DEFAULT_RECORD_BUFFER_SIZE),
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
