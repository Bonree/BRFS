package com.bonree.brfs.disknode.server.handler;

import java.io.File;
import java.io.IOException;
import java.util.BitSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bonree.brfs.common.http.HandleResult;
import com.bonree.brfs.common.http.HandleResultCallback;
import com.bonree.brfs.common.http.HttpMessage;
import com.bonree.brfs.common.http.MessageHandler;
import com.bonree.brfs.common.utils.CloseUtils;
import com.bonree.brfs.common.write.data.FileDecoder;
import com.bonree.brfs.disknode.DiskContext;
import com.bonree.brfs.disknode.data.read.DataFileReader;
import com.bonree.brfs.disknode.data.write.FileWriterManager;
import com.bonree.brfs.disknode.data.write.RecordFileWriter;
import com.bonree.brfs.disknode.data.write.record.RecordCollection;
import com.bonree.brfs.disknode.data.write.record.RecordElement;
import com.bonree.brfs.disknode.data.write.record.RecordElementReader;
import com.bonree.brfs.disknode.data.write.worker.WriteWorker;
import com.bonree.brfs.disknode.utils.Pair;
import com.google.common.base.Splitter;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

public class WritingInfoMessageHandler implements MessageHandler {
	private static final Logger LOG = LoggerFactory.getLogger(WritingInfoMessageHandler.class);
	
	private DiskContext context;
	private FileWriterManager nodeManager;
	
	private Cache<String, Map<Integer, RecordElement>> recordCache = CacheBuilder.newBuilder()
			.maximumSize(10)
			.expireAfterAccess(8, TimeUnit.SECONDS)
			.build();
	
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
			Map<Integer, RecordElement> recordInfo = recordCache.getIfPresent(filePath);
			if(recordInfo == null) {
				Pair<RecordFileWriter, WriteWorker> binding = nodeManager.getBinding(filePath, false);
				if(binding != null) {
					binding.first().flush();
					
					recordInfo = recordCache.getIfPresent(filePath);
					if(recordInfo == null) {
						RecordCollection recordSet = binding.first().getRecordCollection();
						RecordElementReader recordReader = null;
						try {
							recordReader = recordSet.getRecordElementReader();
							recordInfo = new HashMap<Integer, RecordElement>();
							
							for(RecordElement element : recordReader) {
								recordInfo.put(element.getSequence(), element);
							}
							
							recordCache.put(filePath, recordInfo);
						} finally {
							CloseUtils.closeQuietly(recordReader);
						}
					}
				} else {
					File dataFile = new File(filePath);
					if(dataFile.exists()) {
						byte[] bytes = DataFileReader.readFile(dataFile, 0, Integer.MAX_VALUE);
						if(bytes[0] == 0xAC && bytes[1] == 0) {
							recordInfo = new HashMap<Integer, RecordElement>();
							recordInfo.put(0, new RecordElement(0, 0, 2, 0));
							
							List<String> offsetInfos = FileDecoder.getOffsets(bytes);
							int index = 1;
							for(String info : offsetInfos) {
								List<String> parts = Splitter.on('|').splitToList(info);
								int offset = Integer.parseInt(parts.get(0));
								int size = Integer.parseInt(parts.get(1));
								recordInfo.put(index, new RecordElement(index, offset, size, 0));
								index++;
							}
							
							recordCache.put(filePath, recordInfo);
						}
					}
				}
			}
			
			String seqValue = msg.getParams().get("seq");
			if(seqValue != null) {
				if(recordInfo == null) {
					LOG.error("Can not find Record File Writer for file[{}]", filePath);
					result.setSuccess(false);
					result.setCause(new IllegalStateException("The record file of {" + filePath + "} is not existed"));
					callback.completed(result);
					return;
				}
				
				LOG.info("get data by sequence[{}] from file[{}]", seqValue, filePath);
				int seq = Integer.parseInt(seqValue);
				RecordElement element = recordInfo.get(seq);
				byte[] bytes = DataFileReader.readFile(filePath, (int) element.getOffset(), element.getSize());
				
				if(bytes != null) {
					LOG.info("sequence[{}] get all bytes[{}]", seq, bytes.length);
					result.setSuccess(true);
					result.setData(bytes);
					callback.completed(result);
					return;
				}
				
				LOG.info("can not read byte from [{}] with seq[{}]", filePath, seq);
				result.setSuccess(false);
				result.setCause(new Exception("Can't read sequence[" + seq + "]"));
				callback.completed(result);
				return;
			} else {
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
		} catch (IOException e) {
			result.setSuccess(false);
			callback.completed(result);
		}
	}
	
	@Override
	public boolean isValidRequest(HttpMessage message) {
		return true;
	}
}
