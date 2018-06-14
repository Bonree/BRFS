package com.bonree.brfs.disknode.server.handler;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bonree.brfs.common.http.HandleResult;
import com.bonree.brfs.common.http.HandleResultCallback;
import com.bonree.brfs.common.http.HttpMessage;
import com.bonree.brfs.common.http.MessageHandler;
import com.bonree.brfs.disknode.DiskContext;
import com.bonree.brfs.disknode.data.read.DataFileReader;
import com.bonree.brfs.disknode.data.write.record.RecordElement;

public class WritingBytesMessageHandler implements MessageHandler {
	private static final Logger LOG = LoggerFactory.getLogger(WritingBytesMessageHandler.class);
	
	private DiskContext context;
	private SequenceNumberCache cache;
	
	public WritingBytesMessageHandler(DiskContext context, SequenceNumberCache cache) {
		this.context = context;
		this.cache = cache;
	}
	
	private int getSequenceNumber(HttpMessage msg) {
		String seqValue = msg.getParams().get("seq");
		if(seqValue == null) {
			return -1;
		}
		
		try {
			return Integer.parseInt(seqValue);
		} catch(NumberFormatException e) {
			LOG.error("seq error", e);
		}
		
		return -1;
	}

	@Override
	public void handle(HttpMessage msg, HandleResultCallback callback) {
		HandleResult result = new HandleResult();
		
		LOG.info("GET sequence bytes of [{}]", msg.getPath());
		int sequenceNumber = getSequenceNumber(msg);
		
		String filePath = context.getConcreteFilePath(msg.getPath());
		Map<Integer, RecordElement> recordInfo = cache.get(filePath);
		if(recordInfo == null) {
			LOG.error("Can not get record elements for file[{}]", filePath);
			result.setSuccess(false);
			result.setCause(new IllegalStateException("The record file of {" + filePath + "} is not existed"));
			callback.completed(result);
			return;
		}
		
		LOG.info("get data by sequence[{}] from file[{}]", sequenceNumber, filePath);
		RecordElement element = recordInfo.get(sequenceNumber);
		byte[] bytes = DataFileReader.readFile(filePath, (int) element.getOffset(), element.getSize());
		
		if(bytes != null) {
			LOG.info("sequence[{}] get all bytes[{}]", sequenceNumber, bytes.length);
			result.setSuccess(true);
			result.setData(bytes);
			callback.completed(result);
			return;
		}
		
		LOG.info("can not read byte from [{}] with seq[{}]", filePath, sequenceNumber);
		result.setSuccess(false);
		result.setCause(new Exception("Can't read sequence[" + sequenceNumber + "]"));
		callback.completed(result);
	}
	
	@Override
	public boolean isValidRequest(HttpMessage message) {
		return getSequenceNumber(message) >= 0;
	}
}
