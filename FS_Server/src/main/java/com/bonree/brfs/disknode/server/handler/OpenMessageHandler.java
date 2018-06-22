package com.bonree.brfs.disknode.server.handler;

import java.io.IOException;
import java.util.concurrent.ExecutorService;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bonree.brfs.common.net.http.HandleResult;
import com.bonree.brfs.common.net.http.HandleResultCallback;
import com.bonree.brfs.common.net.http.HttpMessage;
import com.bonree.brfs.common.net.http.MessageHandler;
import com.bonree.brfs.common.write.data.FileEncoder;
import com.bonree.brfs.disknode.DiskContext;
import com.bonree.brfs.disknode.data.write.FileWriterManager;
import com.bonree.brfs.disknode.data.write.RecordFileWriter;
import com.bonree.brfs.disknode.data.write.worker.WriteWorker;
import com.bonree.brfs.disknode.utils.Pair;
import com.google.common.primitives.Bytes;
import com.google.common.primitives.Ints;

public class OpenMessageHandler implements MessageHandler {
	private static final Logger LOG = LoggerFactory.getLogger(OpenMessageHandler.class);
	
	private static final int DEFAULT_HEADER_VERSION = 0;
	private static final int DEFAULT_HEADER_TYPE = 0;
	
	private static final int DEFAULT_FILE_CAPACITY = 64 * 1024 * 1024;
	
	private static final int DEFAULT_HEADER_SIZE = 2;
	private static final int DEFAULT_TAILER_SIZE = 9;
	
	private DiskContext diskContext;
	private FileWriterManager writerManager;
	private ExecutorService threadPool;
	
	public OpenMessageHandler(DiskContext diskContext, FileWriterManager writerManager, ExecutorService threadPool) {
		this.diskContext = diskContext;
		this.writerManager = writerManager;
		this.threadPool = threadPool;
	}

	@Override
	public boolean isValidRequest(HttpMessage message) {
		return true;
	}

	@Override
	public void handle(HttpMessage msg, HandleResultCallback callback) {
		threadPool.submit(new Runnable() {
			
			@Override
			public void run() {
				String realPath = diskContext.getConcreteFilePath(msg.getPath());
				int capacity = Integer.parseInt(msg.getParams().getOrDefault("capacity", String.valueOf(DEFAULT_FILE_CAPACITY)));
				LOG.info("open file [{}] with capacity[{}]", realPath, capacity);
				
				Pair<RecordFileWriter, WriteWorker> binding = writerManager.getBinding(realPath, true);
				if(binding == null) {
					LOG.error("get file writer for file[{}] error!", realPath);
					HandleResult result = new HandleResult();
					result.setSuccess(false);
					callback.completed(result);
					return;
				}
				
				try {
					byte[] header = Bytes.concat(FileEncoder.start(), FileEncoder.header(DEFAULT_HEADER_VERSION, DEFAULT_HEADER_TYPE));
					binding.first().updateSequence(0);
					binding.first().write(header);
					binding.first().flush();
					
					HandleResult result = new HandleResult();
					result.setSuccess(true);
					result.setData(Ints.toByteArray(capacity - DEFAULT_HEADER_SIZE - DEFAULT_TAILER_SIZE));
					callback.completed(result);
				} catch (IOException e) {
					LOG.error("write header to file[{}] error!", realPath);
				}
				
				HandleResult result = new HandleResult();
				result.setSuccess(false);
				callback.completed(result);
			}
		});
	}

}
