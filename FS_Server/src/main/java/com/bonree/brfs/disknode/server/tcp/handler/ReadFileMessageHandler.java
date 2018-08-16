package com.bonree.brfs.disknode.server.tcp.handler;

import java.io.File;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bonree.brfs.common.net.tcp.BaseMessage;
import com.bonree.brfs.common.net.tcp.BaseResponse;
import com.bonree.brfs.common.net.tcp.MessageHandler;
import com.bonree.brfs.common.net.tcp.ResponseCode;
import com.bonree.brfs.common.net.tcp.ResponseWriter;
import com.bonree.brfs.common.serialize.ProtoStuffUtils;
import com.bonree.brfs.disknode.DiskContext;
import com.bonree.brfs.disknode.data.read.DataFileReader;
import com.bonree.brfs.disknode.fileformat.FileFormater;
import com.bonree.brfs.disknode.server.tcp.handler.data.ReadFileMessage;
import com.google.common.io.Files;

public class ReadFileMessageHandler implements MessageHandler<BaseResponse> {
	private static final Logger LOG = LoggerFactory.getLogger(ReadFileMessageHandler.class);
	
	public static final String PARAM_READ_OFFSET = "offset";
	public static final String PARAM_READ_LENGTH = "size";
	
	private static final long READ_LENGTH_LIMIT = 100 * 1024;
	
	private DiskContext diskContext;
	private FileFormater fileFormater;
	
	public ReadFileMessageHandler(DiskContext context, FileFormater fileFormater) {
		this.diskContext = context;
		this.fileFormater = fileFormater;
	}

	@Override
	public void handleMessage(BaseMessage baseMessage, ResponseWriter<BaseResponse> writer) {
		ReadFileMessage message = ProtoStuffUtils.deserialize(baseMessage.getBody(), ReadFileMessage.class);
		if(message == null) {
			writer.write(new BaseResponse(ResponseCode.ERROR_PROTOCOL));
			return;
		}
		
		try {
			String filePath = diskContext.getConcreteFilePath(message.getFilePath());
			File dataFile = new File(filePath);
			if(!dataFile.exists() || !dataFile.isFile()) {
				writer.write(new BaseResponse(ResponseCode.ERROR));
				return;
			}
			
			long offset = fileFormater.absoluteOffset(Math.max(0, message.getOffset()));
			int length = (int) Math.min(message.getLength(), fileFormater.maxBodyLength());
			
			LOG.info("read data offset[{}], size[{}] from file[{}]", offset, length, filePath);
			
			byte[] data = null;
			if(length < READ_LENGTH_LIMIT) {
				data = Files.asByteSource(new File(filePath)).slice(offset, length).read();
			} else {
				data = DataFileReader.readFile(filePath, offset, length);
			}
			
			if(data.length != length) {
				writer.write(new BaseResponse(ResponseCode.ERROR));
				return;
			}
			
			BaseResponse response = new BaseResponse(ResponseCode.OK);
			response.setBody(data);
			writer.write(response);
		} catch (Exception e) {
			LOG.error("read message error", e);
			writer.write(new BaseResponse(ResponseCode.ERROR));
		}
	}

}
