package com.bonree.brfs.disknode.server.handler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSONObject;
import com.bonree.brfs.common.http.HandleResult;
import com.bonree.brfs.common.http.HandleResultCallback;
import com.bonree.brfs.common.http.HttpMessage;
import com.bonree.brfs.common.http.MessageHandler;
import com.bonree.brfs.common.utils.BrStringUtils;
import com.bonree.brfs.disknode.DiskContext;
import com.bonree.brfs.disknode.data.write.record.RecordCollection;
import com.bonree.brfs.disknode.data.write.record.RecordCollectionManager;
import com.bonree.brfs.disknode.data.write.record.RecordElement;

public class WritingMetaDataMessageHandler implements MessageHandler {
	private static final Logger LOG = LoggerFactory.getLogger(WritingMetaDataMessageHandler.class);
	
	private DiskContext context;
	private RecordCollectionManager collectionManager;
	
	public WritingMetaDataMessageHandler(DiskContext context, RecordCollectionManager collectionManager) {
		this.context = context;
		this.collectionManager = collectionManager;
	}

	@Override
	public void handle(HttpMessage msg, HandleResultCallback callback) {
		HandleResult result = new HandleResult();
		
		LOG.info("GET META DATA [{}]", msg.getPath());
		String filePath = context.getConcreteFilePath(msg.getPath());
		
		RecordCollection recordSet = collectionManager.getRecordCollectionReadOnly(filePath);
		
		if(recordSet == null) {
			result.setSuccess(false);
			result.setCause(new IllegalStateException("The record file of {" + filePath + "} is not existed"));
			callback.completed(result);
			return;
		}
		
		int length = 0;
		int sequence = -1;
		for(RecordElement ele : recordSet) {
			if(sequence < ele.getSequence()) {
				sequence = ele.getSequence();
			}
			
			length += ele.getSize();
		}
		
		JSONObject json = new JSONObject();
		json.put("seq", sequence);
		json.put("length", length);
		
		result.setSuccess(true);
		result.setData(BrStringUtils.toUtf8Bytes(json.toJSONString()));
		callback.completed(result);
	}

}
