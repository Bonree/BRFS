package com.bonree.brfs.duplication.data.handler;

import com.bonree.brfs.duplication.data.DataDispatcher;
import com.bonree.brfs.duplication.data.DataHandleCallback;
import com.bonree.brfs.duplication.data.DataWriteResult;
import com.bonree.brfs.duplication.server.handler.HandleResultCallback;
import com.bonree.brfs.duplication.server.handler.MessageHandler;

public class WriteDataMessageHandler implements MessageHandler<DataMessage> {
	
	private DataDispatcher dataDispatcher;
	
	public WriteDataMessageHandler(DataDispatcher dataDispatcher) {
		this.dataDispatcher = dataDispatcher;
	}

	@Override
	public void handle(DataMessage msg, HandleResultCallback callback) {
		int storageNameId = msg.getStorageNameId();
		byte[] data = msg.getData();
		
		dataDispatcher.write(storageNameId, data, new DataWriteCallback());
	}

	private class DataWriteCallback implements DataHandleCallback<DataWriteResult> {

		@Override
		public void completed(DataWriteResult result) {
			// TODO Auto-generated method stub
			result.getFid();
		}

		@Override
		public void error(Throwable t) {
			// TODO Auto-generated method stub
			
		}
		
	}
}
