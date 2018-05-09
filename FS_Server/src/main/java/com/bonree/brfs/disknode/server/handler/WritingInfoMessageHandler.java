package com.bonree.brfs.disknode.server.handler;

import java.util.BitSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bonree.brfs.common.http.HandleResult;
import com.bonree.brfs.common.http.HandleResultCallback;
import com.bonree.brfs.common.http.HttpMessage;
import com.bonree.brfs.common.http.MessageHandler;
import com.bonree.brfs.disknode.DiskContext;
import com.bonree.brfs.disknode.data.read.DataFileReader;
import com.bonree.brfs.disknode.data.write.record.RecordCollection;
import com.bonree.brfs.disknode.data.write.record.RecordCollectionManager;
import com.bonree.brfs.disknode.data.write.record.RecordElement;
import com.bonree.brfs.disknode.data.write.record.RecordFileBuilder;

public class WritingInfoMessageHandler implements MessageHandler {
	private static final Logger LOG = LoggerFactory.getLogger(WritingInfoMessageHandler.class);
	
	private DiskContext context;
	private RecordCollectionManager collectionManager;
	
	public WritingInfoMessageHandler(DiskContext context, RecordCollectionManager collectionManager) {
		this.context = context;
		this.collectionManager = collectionManager;
	}

	@Override
	public void handle(HttpMessage msg, HandleResultCallback callback) {
		HandleResult result = new HandleResult();
		
		LOG.info("GET INFO [{}]", msg.getPath());
		String filePath = context.getConcreteFilePath(msg.getPath());
		RecordCollection recordSet = collectionManager.getRecordCollectionReadOnly(filePath);
		
		if(recordSet == null) {
			result.setSuccess(false);
			result.setCause(new IllegalStateException("The record file of {" + filePath + "} is not existed"));
			callback.completed(result);
			return;
		}
		
		String seqValue = msg.getParams().get("seq");
		if(seqValue != null) {
			int seq = Integer.parseInt(seqValue);
			byte[] bytes = readSequenceData(recordSet, seq);
			
			if(bytes != null) {
				result.setSuccess(true);
				result.setData(bytes);
				callback.completed(result);
				return;
			}
			
			result.setSuccess(false);
			result.setCause(new Exception("Can't read sequence[" + seq + "]"));
			callback.completed(result);
			return;
		}
		
		result.setSuccess(true);
		result.setData(getAllSequence(recordSet).toByteArray());
		callback.completed(result);
	}
	
	private byte[] readSequenceData(RecordCollection records, int seq) {
		RecordElement element = null;
		for(RecordElement ele : records) {
			if(ele.getSequence() == seq) {
				element = ele;
				break;
			}
		}
		
		if(element == null) {
			return null;
		}
		
		return DataFileReader.readFile(RecordFileBuilder.reverse(records.recordFile()), (int) element.getOffset(), element.getSize());
	}
	
	private BitSet getAllSequence(RecordCollection records) {
		BitSet seqSet = new BitSet();
		for(RecordElement element : records) {
			seqSet.set(element.getSequence());
		}
		
		return seqSet;
	}
}
