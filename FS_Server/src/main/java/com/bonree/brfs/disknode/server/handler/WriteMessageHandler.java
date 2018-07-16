package com.bonree.brfs.disknode.server.handler;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bonree.brfs.common.net.http.HandleResult;
import com.bonree.brfs.common.net.http.HandleResultCallback;
import com.bonree.brfs.common.net.http.HttpMessage;
import com.bonree.brfs.common.net.http.MessageHandler;
import com.bonree.brfs.common.serialize.ProtoStuffUtils;
import com.bonree.brfs.disknode.DiskContext;
import com.bonree.brfs.disknode.client.WriteDataList;
import com.bonree.brfs.disknode.client.WriteResultList;
import com.bonree.brfs.disknode.data.write.FileWriterManager;
import com.bonree.brfs.disknode.data.write.RecordFileWriter;
import com.bonree.brfs.disknode.data.write.worker.WriteTask;
import com.bonree.brfs.disknode.data.write.worker.WriteWorker;
import com.bonree.brfs.disknode.fileformat.FileFormater;
import com.bonree.brfs.disknode.server.handler.data.WriteData;
import com.bonree.brfs.disknode.server.handler.data.WriteResult;
import com.bonree.brfs.disknode.utils.Pair;

public class WriteMessageHandler implements MessageHandler {
	private static final Logger LOG = LoggerFactory.getLogger(WriteMessageHandler.class);
	
	private DiskContext diskContext;
	private FileWriterManager writerManager;
	private FileFormater fileFormater;
	
	public WriteMessageHandler(DiskContext diskContext, FileWriterManager nodeManager, FileFormater fileFormater) {
		this.diskContext = diskContext;
		this.writerManager = nodeManager;
		this.fileFormater = fileFormater;
	}

	@Override
	public void handle(HttpMessage msg, HandleResultCallback callback) {
		try {
			String realPath = diskContext.getConcreteFilePath(msg.getPath());
			LOG.debug("writing to file [{}]", realPath);
			
			Pair<RecordFileWriter, WriteWorker> binding = writerManager.getBinding(realPath, false);
			if(binding == null) {
				//运行到这，可能时打开文件时失败，导致写数据节点找不到writer
				LOG.warn("no file writer is found, maybe the file[{}] is not opened.", realPath);
				callback.completed(new HandleResult(false));
				return;
			}
			
			binding.second().put(new DataWriteTask(binding, msg, callback));
		} catch (Exception e) {
			LOG.error("EEEERRRRRR", e);
			HandleResult handleResult = new HandleResult();
			handleResult.setSuccess(false);
			handleResult.setCause(e);
			callback.completed(handleResult);
		}
	}
	
	private class DataWriteTask extends WriteTask<WriteResult[]> {
		private HttpMessage message;
		private WriteResult[] results;
		private Pair<RecordFileWriter, WriteWorker> binding;
		private HandleResultCallback callback;
		
		public DataWriteTask(Pair<RecordFileWriter, WriteWorker> binding, HttpMessage message, HandleResultCallback callback) {
			this.binding = binding;
			this.message = message;
			this.callback = callback;
		}

		@Override
		protected WriteResult[] execute() throws Exception {
			WriteDataList dataList = ProtoStuffUtils.deserialize(message.getContent(), WriteDataList.class);
			WriteData[] datas = dataList.getDatas();
			
			results = new WriteResult[datas.length];
			
			RecordFileWriter writer = binding.first();
			for(int i = 0; i < datas.length; i++) {
				WriteData data = datas[i];
				
				LOG.debug("writing file[{}] with data size[{}]", writer.getPath(), data.getBytes().length);
				
				WriteResult result = new WriteResult(fileFormater.relativeOffset(writer.position()), data.getBytes().length);
				writer.write(data.getBytes());
				
				writerManager.flushIfNeeded(writer.getPath());
				results[i] = result;
			}
			
			return results;
		}

		@Override
		protected void onPostExecute(WriteResult[] result) {
			HandleResult handleResult = new HandleResult();
			handleResult.setSuccess(true);
			try {
				WriteResultList resultList = new WriteResultList();
				resultList.setWriteResults(results);
				handleResult.setData(ProtoStuffUtils.serialize(resultList));
			} catch (IOException e) {
				LOG.error("onPostExecute error", e);
			}
			
			callback.completed(handleResult);
		}

		@Override
		protected void onFailed(Throwable cause) {
			HandleResult handleResult = new HandleResult();
			handleResult.setSuccess(true);
			try {
				WriteResultList resultList = new WriteResultList();
				resultList.setWriteResults(results);
				handleResult.setData(ProtoStuffUtils.serialize(resultList));
			} catch (IOException e) {
				LOG.error("onFailed error", e);
			}
			
			callback.completed(handleResult);
		}
		
	}

	@Override
	public boolean isValidRequest(HttpMessage message) {
		return message.getContent().length != 0;
	}
}
