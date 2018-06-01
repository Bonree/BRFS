package com.bonree.brfs.disknode.server.handler;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSONObject;
import com.bonree.brfs.common.http.HandleResult;
import com.bonree.brfs.common.http.HandleResultCallback;
import com.bonree.brfs.common.http.HttpMessage;
import com.bonree.brfs.common.http.MessageHandler;
import com.bonree.brfs.common.utils.BrStringUtils;
import com.bonree.brfs.common.utils.CloseUtils;
import com.bonree.brfs.disknode.DiskContext;
import com.bonree.brfs.disknode.data.write.FileWriterManager;
import com.bonree.brfs.disknode.data.write.RecordFileWriter;
import com.bonree.brfs.disknode.data.write.record.RecordCollection;
import com.bonree.brfs.disknode.data.write.record.RecordElement;
import com.bonree.brfs.disknode.data.write.record.RecordElementReader;
import com.bonree.brfs.disknode.data.write.worker.WriteWorker;
import com.bonree.brfs.disknode.utils.Pair;

public class WritingMetaDataMessageHandler implements MessageHandler {
	private static final Logger LOG = LoggerFactory.getLogger(WritingMetaDataMessageHandler.class);
	
	private DiskContext context;
	private FileWriterManager nodeManager;
	
	public WritingMetaDataMessageHandler(DiskContext context, FileWriterManager nodeManager) {
		this.context = context;
		this.nodeManager = nodeManager;
	}

	@Override
	public void handle(HttpMessage msg, HandleResultCallback callback) {
		HandleResult result = new HandleResult();
		
		LOG.info("GET META DATA [{}]", msg.getPath());
		String filePath = context.getConcreteFilePath(msg.getPath());
		
		Pair<RecordFileWriter, WriteWorker> binding = nodeManager.getBinding(filePath, false);
		
		if(binding == null) {
			LOG.error("Can not find Record File Writer for file[{}]", filePath);
			result.setSuccess(false);
			result.setCause(new IllegalStateException("The record file of {" + filePath + "} is not existed"));
			callback.completed(result);
			return;
		}
		
		RecordElementReader recordReader = null;
		try {
			binding.first().flush();
			RecordCollection recordSet = binding.first().getRecordCollection();
			
			recordReader = recordSet.getRecordElementReader();
			RecordElement lastEle = new RecordElement(-1, 0);
			for(RecordElement ele : recordReader) {
				if(lastEle.getSequence() < ele.getSequence()) {
					lastEle = ele;
				}
			}
			
			if(lastEle.getSize() == 0) {
				LOG.error("No record elements exists about file[{}]", filePath);
				result.setSuccess(false);
				result.setCause(new Exception("no record element exists!"));
				callback.completed(result);
				return;
			}
			
			JSONObject json = new JSONObject();
			json.put("seq", lastEle.getSequence());
			json.put("length", lastEle.getOffset() + lastEle.getSize());
			
			result.setSuccess(true);
			result.setData(BrStringUtils.toUtf8Bytes(json.toJSONString()));
			callback.completed(result);
		} catch (IOException e) {
			e.printStackTrace();
			
			result.setSuccess(false);
			result.setCause(e);
			callback.completed(result);
		} finally {
			CloseUtils.closeQuietly(recordReader);
		}
	}
	
	@Override
	public boolean isValidRequest(HttpMessage message) {
		return true;
	}

}
