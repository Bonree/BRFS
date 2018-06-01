package com.bonree.brfs.disknode.server.handler;

import java.io.IOException;
import java.util.BitSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bonree.brfs.common.http.HandleResult;
import com.bonree.brfs.common.http.HandleResultCallback;
import com.bonree.brfs.common.http.HttpMessage;
import com.bonree.brfs.common.http.MessageHandler;
import com.bonree.brfs.disknode.DiskContext;
import com.bonree.brfs.disknode.data.read.DataFileReader;
import com.bonree.brfs.disknode.data.write.FileWriterManager;
import com.bonree.brfs.disknode.data.write.RecordFileWriter;
import com.bonree.brfs.disknode.data.write.record.RecordCollection;
import com.bonree.brfs.disknode.data.write.record.RecordElement;
import com.bonree.brfs.disknode.data.write.record.RecordFileBuilder;
import com.bonree.brfs.disknode.data.write.worker.WriteWorker;
import com.bonree.brfs.disknode.utils.Pair;

public class WritingInfoMessageHandler implements MessageHandler {
	private static final Logger LOG = LoggerFactory.getLogger(WritingInfoMessageHandler.class);
	
	private DiskContext context;
	private FileWriterManager nodeManager;
	
	public WritingInfoMessageHandler(DiskContext context, FileWriterManager nodeManager) {
		this.context = context;
		this.nodeManager = nodeManager;
	}

	@Override
	public void handle(HttpMessage msg, HandleResultCallback callback) {
		HandleResult result = new HandleResult();
		
		LOG.info("GET INFO [{}]", msg.getPath());
		String filePath = context.getConcreteFilePath(msg.getPath());
		try {
			Pair<RecordFileWriter, WriteWorker> binding = nodeManager.getBinding(filePath, false);
			
			if(binding == null) {
				LOG.error("Can not find Record File Writer for file[{}]", filePath);
				result.setSuccess(false);
				result.setCause(new IllegalStateException("The record file of {" + filePath + "} is not existed"));
				callback.completed(result);
				return;
			}
			
			binding.first().flush();
			RecordCollection recordSet = binding.first().getRecordCollection();
			String seqValue = msg.getParams().get("seq");
			if(seqValue != null) {
				LOG.info("get data by sequence[{}] from file[{}]", seqValue, filePath);
				int seq = Integer.parseInt(seqValue);
				byte[] bytes = readSequenceData(recordSet, seq);
				
				if(bytes != null) {
					LOG.info("sequence[{}] get all bytes[{}]", seq, bytes.length);
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
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	@Override
	public boolean isValidRequest(HttpMessage message) {
		return true;
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
		
		LOG.info("get all sequence from file[{}] ,total[{}]", records.recordFile().getAbsolutePath(), seqSet.cardinality());
		
		return seqSet;
	}
}
