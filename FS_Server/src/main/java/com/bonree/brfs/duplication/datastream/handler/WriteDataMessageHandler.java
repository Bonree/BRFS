package com.bonree.brfs.duplication.datastream.handler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bonree.brfs.common.net.http.HandleResult;
import com.bonree.brfs.common.net.http.HandleResultCallback;
import com.bonree.brfs.common.net.http.HttpMessage;
import com.bonree.brfs.common.net.http.MessageHandler;
import com.bonree.brfs.common.serialize.ProtoStuffUtils;
import com.bonree.brfs.common.utils.JsonUtils;
import com.bonree.brfs.common.utils.JsonUtils.JsonException;
import com.bonree.brfs.common.write.data.DataItem;
import com.bonree.brfs.common.write.data.WriteDataMessage;
import com.bonree.brfs.duplication.datastream.writer.StorageRegionWriteCallback;
import com.bonree.brfs.duplication.datastream.writer.StorageRegionWriter;

public class WriteDataMessageHandler implements MessageHandler {
	private static final Logger LOG = LoggerFactory.getLogger(WriteDataMessageHandler.class);
	
	private StorageRegionWriter writer;
	
	public WriteDataMessageHandler(StorageRegionWriter writer) {
		this.writer = writer;
	}

	@Override
	public void handle(HttpMessage msg, HandleResultCallback callback) {
		WriteDataMessage writeMsg;
		try {
			writeMsg = ProtoStuffUtils.deserializeThrowable(msg.getContent(), WriteDataMessage.class);
			
			DataItem[] items = writeMsg.getItems();
			LOG.debug("Writing DataItem[{}]", items.length);
			
			if(items == null || items.length == 0) {
				callback.completed(new HandleResult(true));
				return;
			}
			
			writer.write(writeMsg.getStorageNameId(), items, new DataWriteCallback(callback));
		} catch (Exception e) {
			LOG.error("handle write data message error", e);
			callback.completed(new HandleResult(false));
		}
	}

	private class DataWriteCallback implements StorageRegionWriteCallback {
		private HandleResultCallback callback;
		
		public DataWriteCallback(HandleResultCallback callback) {
			this.callback = callback;
		}

		@Override
		public void complete(String[] fids) {
			HandleResult result = new HandleResult();
			
			try {
				result.setData(JsonUtils.toJsonBytes(fids));
				result.setSuccess(true);
			} catch (JsonException e) {
				LOG.error("can not json fids", e);
				result.setSuccess(false);
			}
			
			callback.completed(result);
		}

		@Override
		public void complete(String fid) {
			return;
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
