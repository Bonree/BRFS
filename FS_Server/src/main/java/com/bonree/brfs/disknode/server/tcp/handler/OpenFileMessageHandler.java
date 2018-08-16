package com.bonree.brfs.disknode.server.tcp.handler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bonree.brfs.common.net.tcp.BaseMessage;
import com.bonree.brfs.common.net.tcp.BaseResponse;
import com.bonree.brfs.common.net.tcp.MessageHandler;
import com.bonree.brfs.common.net.tcp.ResponseCode;
import com.bonree.brfs.common.net.tcp.ResponseWriter;
import com.bonree.brfs.common.serialize.ProtoStuffUtils;
import com.bonree.brfs.configuration.Configs;
import com.bonree.brfs.configuration.units.DataNodeConfigs;
import com.bonree.brfs.disknode.DiskContext;
import com.bonree.brfs.disknode.data.write.FileWriterManager;
import com.bonree.brfs.disknode.data.write.RecordFileWriter;
import com.bonree.brfs.disknode.data.write.worker.WriteWorker;
import com.bonree.brfs.disknode.fileformat.FileFormater;
import com.bonree.brfs.disknode.fileformat.impl.SimpleFileFormater;
import com.bonree.brfs.disknode.server.tcp.handler.data.OpenFileMessage;
import com.bonree.brfs.disknode.utils.Pair;
import com.google.common.primitives.Longs;

public class OpenFileMessageHandler implements MessageHandler<BaseResponse> {
	private static final Logger LOG = LoggerFactory.getLogger(OpenFileMessageHandler.class);
	
	private DiskContext diskContext;
	private FileWriterManager writerManager;
	
	private static final long MAX_CAPACITY = Configs.getConfiguration().GetConfig(DataNodeConfigs.CONFIG_FILE_MAX_CAPACITY);
	
	public OpenFileMessageHandler(DiskContext diskContext, FileWriterManager writerManager) {
		this.diskContext = diskContext;
		this.writerManager = writerManager;
	}

	@Override
	public void handleMessage(BaseMessage baseMessage, ResponseWriter<BaseResponse> writer) {
		OpenFileMessage message = ProtoStuffUtils.deserialize(baseMessage.getBody(), OpenFileMessage.class);
		if(message == null) {
			writer.write(new BaseResponse(ResponseCode.ERROR_PROTOCOL));
			return;
		}
		
		FileFormater fileFormater = new SimpleFileFormater(Math.min(message.getCapacity(), MAX_CAPACITY));
		
		String realPath = diskContext.getConcreteFilePath(message.getFilePath());
		LOG.info("open file [{}]", realPath);
		
		Pair<RecordFileWriter, WriteWorker> binding = writerManager.getBinding(realPath, true);
		if(binding == null) {
			LOG.error("get file writer for file[{}] error!", realPath);
			writer.write(new BaseResponse(ResponseCode.ERROR));
			return;
		}
		
		try {
			binding.first().write(fileFormater.fileHeader().getBytes());
			binding.first().flush();
			
			BaseResponse response = new BaseResponse(ResponseCode.OK);
			response.setBody(Longs.toByteArray(fileFormater.maxBodyLength()));
			writer.write(response);
		} catch (Exception e) {
			LOG.error("write header to file[{}] error!", realPath);
			writer.write(new BaseResponse(ResponseCode.ERROR));
		}
	}

}
