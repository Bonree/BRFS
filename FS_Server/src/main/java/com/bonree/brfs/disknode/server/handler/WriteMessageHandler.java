package com.bonree.brfs.disknode.server.handler;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bonree.brfs.common.http.HandleResult;
import com.bonree.brfs.common.http.HandleResultCallback;
import com.bonree.brfs.common.http.HttpMessage;
import com.bonree.brfs.common.http.MessageHandler;
import com.bonree.brfs.common.utils.ProtoStuffUtils;
import com.bonree.brfs.common.utils.ThreadPoolUtil;
import com.bonree.brfs.disknode.DiskContext;
import com.bonree.brfs.disknode.client.WriteDataList;
import com.bonree.brfs.disknode.client.WriteResultList;
import com.bonree.brfs.disknode.data.write.FileWriterManager;
import com.bonree.brfs.disknode.data.write.RecordFileWriter;
import com.bonree.brfs.disknode.data.write.worker.WriteTask;
import com.bonree.brfs.disknode.data.write.worker.WriteWorker;
import com.bonree.brfs.disknode.server.handler.data.WriteData;
import com.bonree.brfs.disknode.server.handler.data.WriteResult;
import com.bonree.brfs.disknode.utils.Pair;

public class WriteMessageHandler implements MessageHandler {
	private static final Logger LOG = LoggerFactory.getLogger(WriteMessageHandler.class);
	
	private DiskContext diskContext;
	private FileWriterManager writerManager;
	
	public WriteMessageHandler(DiskContext diskContext, FileWriterManager nodeManager) {
		this.diskContext = diskContext;
		this.writerManager = nodeManager;
	}

	@Override
	public void handle(HttpMessage msg, HandleResultCallback callback) {
		try {
			String realPath = diskContext.getConcreteFilePath(msg.getPath());
			LOG.debug("writing to file [{}]", realPath);
			
			if(msg.getContent().length == 0) {
				throw new IllegalArgumentException("Writing data is Empty!!");
			}
			
			WriteDataList dataList = ProtoStuffUtils.deserialize(msg.getContent(), WriteDataList.class);
			Pair<RecordFileWriter, WriteWorker> binding = writerManager.getBinding(realPath, true);
			if(binding == null) {
				throw new IllegalStateException("File Writer is null");
			}
			
			WriteData[] datas = dataList.getDatas();
			binding.second().put(new DataWriteTask(binding, datas, callback));
		} catch (Exception e) {
			LOG.error("EEEERRRRRR", e);
			HandleResult handleResult = new HandleResult();
			handleResult.setSuccess(false);
			handleResult.setCause(e);
			callback.completed(handleResult);
		}
	}
	
	private class DataWriteTask extends WriteTask<WriteResult[]> {
		private WriteData[] dataList;
		private WriteResult[] results;
		private Pair<RecordFileWriter, WriteWorker> binding;
		private HandleResultCallback callback;
		
		public DataWriteTask(Pair<RecordFileWriter, WriteWorker> binding, WriteData[] datas, HandleResultCallback callback) {
			this.binding = binding;
			this.dataList = datas;
			this.results = new WriteResult[datas.length];
			this.callback = callback;
		}

		@Override
		protected WriteResult[] execute() throws Exception {
			LOG.info("start writing...");
			RecordFileWriter writer = binding.first();
			for(int i = 0; i < dataList.length; i++) {
				WriteData data = dataList[i];
				
				LOG.info("writing file[{}] with data seq[{}], size[{}]", writer.getPath(), data.getDiskSequence(), data.getBytes().length);
				
				WriteResult result = new WriteResult();
				result.setSequence(data.getDiskSequence());
				
				writer.updateSequence(data.getDiskSequence());
				writer.write(data.getBytes());
				
				writerManager.flushIfNeeded(binding);
				
				result.setSize(data.getBytes().length);
				results[i] = result;
			}
			
			return results;
		}

		@Override
		protected void onPostExecute(WriteResult[] result) {
			ThreadPoolUtil.commonPool().execute(new Runnable() {
				
				@Override
				public void run() {
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
			});
		}

		@Override
		protected void onFailed(Throwable e) {
			ThreadPoolUtil.commonPool().execute(new Runnable() {
				
				@Override
				public void run() {
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
			});
		}
		
	}

	@Override
	public boolean isValidRequest(HttpMessage message) {
		return true;
	}
}
