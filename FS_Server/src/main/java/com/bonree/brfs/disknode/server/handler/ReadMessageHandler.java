package com.bonree.brfs.disknode.server.handler;

import java.io.File;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bonree.brfs.common.net.http.HandleResult;
import com.bonree.brfs.common.net.http.HandleResultCallback;
import com.bonree.brfs.common.net.http.HttpMessage;
import com.bonree.brfs.common.net.http.MessageHandler;
import com.bonree.brfs.disknode.DiskContext;
import com.bonree.brfs.disknode.data.read.DataFileReader;
import com.bonree.brfs.disknode.fileformat.FileFormater;
import com.google.common.io.Files;

public class ReadMessageHandler implements MessageHandler {
	private static final Logger LOG = LoggerFactory.getLogger(ReadMessageHandler.class);
	
	public static final String PARAM_READ_OFFSET = "offset";
	public static final String PARAM_READ_LENGTH = "size";
	
	private static final long READ_LENGTH_LIMIT = 100 * 1024;
	
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
			File dataFile = new File(filePath);
			if(!dataFile.exists() || !dataFile.isFile()) {
				result.setSuccess(false);
				return;
			}
			
			String offsetParam = msg.getParams().get(PARAM_READ_OFFSET);
			String lengthParam = msg.getParams().get(PARAM_READ_LENGTH);
			int offset = offsetParam == null ? 0 : Integer.parseInt(offsetParam);
			int length = (int) (lengthParam == null ? fileFormater.maxBodyLength() : Integer.parseInt(lengthParam));
			
			length = (int) Math.min(length, fileFormater.maxBodyLength());
			
			LOG.info("read data offset[{}], size[{}] from file[{}]", offset, length, filePath);
			
			byte[] data = null;
			if(length < READ_LENGTH_LIMIT) {
				data = Files.asByteSource(new File(filePath)).slice(fileFormater.absoluteOffset(offset), length).read();
			} else {
				data = DataFileReader.readFile(filePath, fileFormater.absoluteOffset(offset), length);
			}
			
			result.setSuccess(data.length != length ? false : true);
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
