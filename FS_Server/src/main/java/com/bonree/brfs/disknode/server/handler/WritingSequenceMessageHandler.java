package com.bonree.brfs.disknode.server.handler;

import java.util.BitSet;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bonree.brfs.common.http.HandleResult;
import com.bonree.brfs.common.http.HandleResultCallback;
import com.bonree.brfs.common.http.HttpMessage;
import com.bonree.brfs.common.http.MessageHandler;
import com.bonree.brfs.disknode.DiskContext;
import com.bonree.brfs.disknode.data.write.record.RecordElement;

public class WritingSequenceMessageHandler implements MessageHandler {
	private static final Logger LOG = LoggerFactory.getLogger(WritingSequenceMessageHandler.class);
	
	private DiskContext context;
	private SequenceNumberCache cache;
	
	public WritingSequenceMessageHandler(DiskContext context, SequenceNumberCache cache) {
		this.context = context;
		this.cache = cache;
	}

	@Override
	public void handle(HttpMessage msg, HandleResultCallback callback) {
		HandleResult result = new HandleResult();
		
		LOG.info("GET sequences of file[{}]", msg.getPath());
		String filePath = context.getConcreteFilePath(msg.getPath());
		
		Map<Integer, RecordElement> recordInfo = cache.get(filePath, true);
		if(recordInfo == null) {
			LOG.info("can not get record elements of file[{}]", filePath);
			result.setSuccess(false);
			callback.completed(result);
			return;
		}
		
		//获取所有文件序列号
		BitSet seqSet = new BitSet();
		if(recordInfo != null) {
			for(Integer seq : recordInfo.keySet()) {
				seqSet.set(seq);
			}
		}
		
		LOG.info("get all sequence from file[{}] ,total[{}]", filePath, seqSet.cardinality());
		result.setSuccess(true);
		result.setData(seqSet.toByteArray());
		callback.completed(result);
	}
	
	@Override
	public boolean isValidRequest(HttpMessage message) {
		return true;
	}
}
