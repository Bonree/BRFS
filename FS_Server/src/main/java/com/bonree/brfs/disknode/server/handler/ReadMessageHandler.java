package com.bonree.brfs.disknode.server.handler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bonree.brfs.common.net.http.HandleResult;
import com.bonree.brfs.common.net.http.HandleResultCallback;
import com.bonree.brfs.common.net.http.HttpMessage;
import com.bonree.brfs.common.net.http.MessageHandler;
import com.bonree.brfs.disknode.DiskContext;
import com.bonree.brfs.disknode.data.read.DataFileReader;
import com.bonree.brfs.disknode.fileformat.FileFormater;

public class ReadMessageHandler implements MessageHandler {
	private static final Logger LOG = LoggerFactory.getLogger(ReadMessageHandler.class);
	
	public static final String PARAM_READ_OFFSET = "offset";
	public static final String PARAM_READ_LENGTH = "size";
	
	private DiskContext diskContext;
	private FileFormater fileFormater;
	
	public ReadMessageHandler(DiskContext context, FileFormater fileFormater) {
		this.diskContext = context;
		this.fileFormater = fileFormater;
	}

	@Override
	public void handle(HttpMessage msg, HandleResultCallback callback) {
		HandleResult result = new HandleResult();
		
		try {
			String filePath = diskContext.getConcreteFilePath(msg.getPath());
			
			String offsetParam = msg.getParams().get(PARAM_READ_OFFSET);
			String lengthParam = msg.getParams().get(PARAM_READ_LENGTH);
			int offset = offsetParam == null ? 0 : Integer.parseInt(offsetParam);
			int length = (int) (lengthParam == null ? fileFormater.maxBodyLength() : Integer.parseInt(lengthParam));
			
			LOG.info("read data offset[{}], size[{}]", offset, length);
			
			byte[] data = DataFileReader.readFile(filePath, fileFormater.absoluteOffset(offset), length);
			
			result.setSuccess(data.length == 0 ? false : true);
			result.setData(data);
		} catch (Exception e) {
			LOG.error("read message error", e);
			result.setSuccess(false);
		} finally {
			callback.completed(result);
		}
		
	}
	
	@Override
	public boolean isValidRequest(HttpMessage message) {
		return true;
	}

}
