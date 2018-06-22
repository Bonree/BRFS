package com.bonree.brfs.disknode.server.handler;

import java.util.BitSet;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bonree.brfs.common.net.http.HandleResult;
import com.bonree.brfs.common.net.http.HandleResultCallback;
import com.bonree.brfs.common.net.http.HttpMessage;
import com.bonree.brfs.common.net.http.MessageHandler;
import com.bonree.brfs.disknode.DiskContext;
import com.bonree.brfs.disknode.data.write.record.RecordElement;
import com.bonree.brfs.disknode.server.handler.SequenceNumberCache.CacheCallback;

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
		
		
		LOG.info("GET sequences of file[{}]", msg.getPath());
		String filePath = context.getConcreteFilePath(msg.getPath());
		
		cache.get(filePath, true, new CacheCallback() {
			
			@Override
			public void elementReceived(Map<Integer, RecordElement> recordInfo) {
				if(recordInfo == null) {
					LOG.info("can not get record elements of file[{}]", filePath);
					callback.completed(new HandleResult(false));
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
				HandleResult result = new HandleResult(true);
				result.setData(seqSet.toByteArray());
				callback.completed(result);
			}
		});
	}
	
	@Override
	public boolean isValidRequest(HttpMessage message) {
		return true;
	}
}
