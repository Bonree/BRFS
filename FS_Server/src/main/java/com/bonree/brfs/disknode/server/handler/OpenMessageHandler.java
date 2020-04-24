package com.bonree.brfs.disknode.server.handler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bonree.brfs.common.net.http.HandleResult;
import com.bonree.brfs.common.net.http.HandleResultCallback;
import com.bonree.brfs.common.net.http.HttpMessage;
import com.bonree.brfs.common.net.http.MessageHandler;
import com.bonree.brfs.configuration.Configs;
import com.bonree.brfs.configuration.units.DataNodeConfigs;
import com.bonree.brfs.disknode.DiskContext;
import com.bonree.brfs.disknode.data.write.FileWriterManager;
import com.bonree.brfs.disknode.data.write.RecordFileWriter;
import com.bonree.brfs.disknode.data.write.worker.WriteWorker;
import com.bonree.brfs.disknode.fileformat.FileFormater;
import com.bonree.brfs.disknode.fileformat.impl.SimpleFileFormater;
import com.bonree.brfs.disknode.utils.Pair;
import com.google.common.primitives.Longs;

public class OpenMessageHandler implements MessageHandler {
	private static final Logger LOG = LoggerFactory.getLogger(OpenMessageHandler.class);
	
	private DiskContext diskContext;
	private FileWriterManager writerManager;
	
	private static final long MAX_CAPACITY = Configs.getConfiguration().getConfig(DataNodeConfigs.CONFIG_FILE_MAX_CAPACITY);
	
	public OpenMessageHandler(DiskContext diskContext, FileWriterManager writerManager) {
		this.diskContext = diskContext;
		this.writerManager = writerManager;
	}

	@Override
	public boolean isValidRequest(HttpMessage message) {
		return true;
	}

	@Override
	public void handle(HttpMessage msg, HandleResultCallback callback) {
		HandleResult result = new HandleResult();
		String realPath = null;
		try {
			String capacityParam = msg.getParams().get("capacity");
			if(capacityParam == null) {
				result.setSuccess(false);
				return;
			}
			
			long capacity = Long.parseLong(capacityParam);
			FileFormater fileFormater = new SimpleFileFormater(Math.min(capacity, MAX_CAPACITY));
			
			realPath = diskContext.getConcreteFilePath(msg.getPath());
			LOG.info("open file [{}]", realPath);
			
			Pair<RecordFileWriter, WriteWorker> binding = writerManager.getBinding(realPath, true);
			if(binding == null) {
				LOG.error("get file writer for file[{}] error!", realPath);
				result.setSuccess(false);
				return;
			}
			
			binding.first().write(fileFormater.fileHeader().getBytes());
			binding.first().flush();
			
			result.setData(Longs.toByteArray(fileFormater.maxBodyLength()));
			result.setSuccess(true);
		} catch (Exception e) {
			LOG.error("write header to file[{}] error!", realPath);
			result.setSuccess(false);
		} finally {
			callback.completed(result);
		}
	}

}
