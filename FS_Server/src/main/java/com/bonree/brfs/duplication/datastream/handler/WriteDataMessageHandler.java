package com.bonree.brfs.duplication.datastream.handler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bonree.brfs.common.net.http.HandleResult;
import com.bonree.brfs.common.net.http.HandleResultCallback;
import com.bonree.brfs.common.net.http.HttpMessage;
import com.bonree.brfs.common.net.http.MessageHandler;
import com.bonree.brfs.common.proto.FileDataProtos.FileContent;
import com.bonree.brfs.common.serialize.ProtoStuffUtils;
import com.bonree.brfs.common.utils.BrStringUtils;
import com.bonree.brfs.common.utils.JsonUtils;
import com.bonree.brfs.common.write.data.DataItem;
import com.bonree.brfs.common.write.data.FileEncoder;
import com.bonree.brfs.common.write.data.WriteDataMessage;
import com.bonree.brfs.duplication.datastream.writer.StorageRegionWriteCallback;
import com.bonree.brfs.duplication.datastream.writer.StorageRegionWriter;
import com.google.protobuf.ByteString;

public class WriteDataMessageHandler implements MessageHandler {
	private static final Logger LOG = LoggerFactory.getLogger(WriteDataMessageHandler.class);
	
	private StorageRegionWriter writer;
	
	public WriteDataMessageHandler(StorageRegionWriter writer) {
		this.writer = writer;
	}

	@Override
	public void handle(HttpMessage msg, HandleResultCallback callback) {
		WriteDataMessage writeMsg = ProtoStuffUtils.deserialize(msg.getContent(), WriteDataMessage.class);
		
		DataItem[] items = writeMsg.getItems();
		LOG.debug("Writing DataItem[{}]", items.length);
		
		if(items == null || items.length == 0) {
			callback.completed(new HandleResult(true));
			return;
		}
		
		for(DataItem item : items) {
			FileContent content = FileContent.newBuilder()
					.setCompress(0)
					.setDescription("")
					.setData(ByteString.copyFrom(item.getBytes()))
					.setCrcFlag(false)
					.setCrcCheckCode(0)
					.build();
			
			try {
				byte[] bytes = FileEncoder.contents(content);
				item.setBytes(bytes);
			} catch (Exception e) {
				LOG.error("encode file content error", e);
			}
		}
		
		writer.write(writeMsg.getStorageNameId(), items, new DataWriteCallback(callback));
	}

	private class DataWriteCallback implements StorageRegionWriteCallback {
		private HandleResultCallback callback;
		
		public DataWriteCallback(HandleResultCallback callback) {
			this.callback = callback;
		}

		@Override
		public void complete(String[] fids) {
			HandleResult result = new HandleResult();
			result.setSuccess(true);
			
			result.setData(BrStringUtils.toUtf8Bytes(JsonUtils.toJsonString(fids)));
			
			callback.completed(result);
		}

		@Override
		public void error() {
			callback.completed(new HandleResult(false));
		}
		
	}

	@Override
	public boolean isValidRequest(HttpMessage message) {
		return true;
	}
}
