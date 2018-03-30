package com.bonree.brfs.duplication.datastream.handler;

import com.bonree.brfs.common.http.HandleResult;
import com.bonree.brfs.common.http.HandleResultCallback;
import com.bonree.brfs.common.http.MessageHandler;
import com.bonree.brfs.common.utils.StringUtils;
import com.bonree.brfs.duplication.datastream.DuplicateWriter;
import com.bonree.brfs.duplication.datastream.DataHandleCallback;
import com.bonree.brfs.duplication.datastream.tasks.DataWriteResult;

public class WriteDataMessageHandler implements MessageHandler<DataMessage> {
	
	private DuplicateWriter duplicateWriter;
	
	public WriteDataMessageHandler(DuplicateWriter duplicateWriter) {
		this.duplicateWriter = duplicateWriter;
	}

	@Override
	public void handle(DataMessage msg, HandleResultCallback callback) {
		DataItem[] items = msg.getItems();
		if(items == null || items.length == 0) {
			HandleResult result = new HandleResult();
			result.setSuccess(true);
			callback.completed(result);
			return;
		}
		
		duplicateWriter.write(msg.getStorageNameId(), items, new DataWriteCallback(callback));
	}

	private class DataWriteCallback implements DataHandleCallback<DataWriteResult> {
		
		private HandleResultCallback callback;
		
		public DataWriteCallback(HandleResultCallback callback) {
			this.callback = callback;
		}

		@Override
		public void completed(DataWriteResult writeResult) {
			HandleResult result = new HandleResult();
			result.setSuccess(true);
			result.setData(StringUtils.toUtf8Bytes(writeResult.getFid()));
			
			callback.completed(result);
		}

		@Override
		public void error(Throwable t) {
			HandleResult result = new HandleResult();
			result.setSuccess(false);
			result.setCause(t);
			
			callback.completed(result);
		}
		
	}
}
