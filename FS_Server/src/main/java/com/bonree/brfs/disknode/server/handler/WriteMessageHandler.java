package com.bonree.brfs.disknode.server.handler;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bonree.brfs.common.net.http.HandleResult;
import com.bonree.brfs.common.net.http.HandleResultCallback;
import com.bonree.brfs.common.net.http.HttpMessage;
import com.bonree.brfs.common.net.http.MessageHandler;
import com.bonree.brfs.common.timer.TimeCounter;
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
			
			Pair<RecordFileWriter, WriteWorker> binding = writerManager.getBinding(realPath, false);
			if(binding == null) {
				//运行到这，可能时打开文件时失败，导致写数据节点找不到writer
				LOG.warn("no file writer is found, maybe the file[{}] is not opened.", realPath);
				callback.completed(new HandleResult(false));
				return;
			}
			
			TimeCounter counter = new TimeCounter("write_data", TimeUnit.MILLISECONDS);
			counter.begin();
			
			if(msg.getContent().length == 0) {
				throw new IllegalArgumentException("Writing data is Empty!!");
			}
			
			LOG.info(counter.report(2));
			
			binding.second().put(new DataWriteTask(binding, msg, callback));
			
			LOG.info(counter.report(1));
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
		
		private TimeCounter counter = new TimeCounter("DataWriteTask", TimeUnit.MILLISECONDS);
		
		public DataWriteTask(Pair<RecordFileWriter, WriteWorker> binding, HttpMessage message, HandleResultCallback callback) {
			this.binding = binding;
			this.message = message;
			this.callback = callback;
		}

		@Override
		protected WriteResult[] execute() throws Exception {
			LOG.info("start writing...");
			WriteDataList dataList = ProtoStuffUtils.deserialize(message.getContent(), WriteDataList.class);
			WriteData[] datas = dataList.getDatas();
			
			results = new WriteResult[datas.length];
			
			RecordFileWriter writer = binding.first();
			for(int i = 0; i < datas.length; i++) {
				WriteData data = datas[i];
				
				LOG.info("writing file[{}] with data seq[{}], size[{}]", writer.getPath(), data.getDiskSequence(), data.getBytes().length);
				
				WriteResult result = new WriteResult();
				result.setSequence(data.getDiskSequence());
				
				writer.updateSequence(data.getDiskSequence());
				writer.write(data.getBytes());
				
				writerManager.flushIfNeeded(writer.getPath());
				
				result.setSize(data.getBytes().length);
				results[i] = result;
			}
			
			return results;
		}

		@Override
		protected void onPostExecute(WriteResult[] result) {
			LOG.info(counter.report(0));
			
			TimeCounter runCounter = new TimeCounter("postResult", TimeUnit.MILLISECONDS);
			runCounter.begin();
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
					LOG.info(runCounter.report(0));
				}
			});
		}

		@Override
		protected void onFailed(Throwable e) {
			LOG.info(counter.report(1));
			TimeCounter runCounter = new TimeCounter("postFailed", TimeUnit.MILLISECONDS);
			runCounter.begin();
			
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
					LOG.info(runCounter.report(0));
				}
			});
		}
		
	}

	@Override
	public boolean isValidRequest(HttpMessage message) {
		return true;
	}
}
