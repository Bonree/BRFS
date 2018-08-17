package com.bonree.brfs.disknode.server.tcp.handler;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bonree.brfs.common.net.tcp.BaseMessage;
import com.bonree.brfs.common.net.tcp.BaseResponse;
import com.bonree.brfs.common.net.tcp.MessageHandler;
import com.bonree.brfs.common.net.tcp.ResponseCode;
import com.bonree.brfs.common.net.tcp.ResponseWriter;
import com.bonree.brfs.common.serialize.ProtoStuffUtils;
import com.bonree.brfs.disknode.DiskContext;
import com.bonree.brfs.disknode.client.WriteResult;
import com.bonree.brfs.disknode.client.WriteResultList;
import com.bonree.brfs.disknode.data.write.FileWriterManager;
import com.bonree.brfs.disknode.data.write.RecordFileWriter;
import com.bonree.brfs.disknode.data.write.worker.WriteTask;
import com.bonree.brfs.disknode.data.write.worker.WriteWorker;
import com.bonree.brfs.disknode.fileformat.FileFormater;
import com.bonree.brfs.disknode.server.tcp.handler.data.WriteFileData;
import com.bonree.brfs.disknode.server.tcp.handler.data.WriteFileMessage;
import com.bonree.brfs.disknode.utils.Pair;

public class WriteFileMessageHandler implements MessageHandler<BaseResponse> {
	private static final Logger LOG = LoggerFactory.getLogger(WriteFileMessageHandler.class);
	
	private DiskContext diskContext;
	private FileWriterManager writerManager;
	private FileFormater fileFormater;
	
	public WriteFileMessageHandler(DiskContext diskContext, FileWriterManager nodeManager, FileFormater fileFormater) {
		this.diskContext = diskContext;
		this.writerManager = nodeManager;
		this.fileFormater = fileFormater;
	}

	@Override
	public void handleMessage(BaseMessage baseMessage, ResponseWriter<BaseResponse> writer) {
		WriteFileMessage message = ProtoStuffUtils.deserialize(baseMessage.getBody(), WriteFileMessage.class);
		if(message == null) {
			writer.write(new BaseResponse(ResponseCode.ERROR_PROTOCOL));
			return;
		}
		
		try {
			String realPath = diskContext.getConcreteFilePath(message.getFilePath());
			LOG.debug("writing to file [{}]", realPath);
			
			Pair<RecordFileWriter, WriteWorker> binding = writerManager.getBinding(realPath, false);
			if(binding == null) {
				//运行到这，可能时打开文件时失败，导致写数据节点找不到writer
				LOG.warn("no file writer is found, maybe the file[{}] is not opened.", realPath);
				writer.write(new BaseResponse(ResponseCode.ERROR));
				return;
			}
			
			binding.second().put(new DataWriteTask(binding, message, writer));
		} catch (Exception e) {
			LOG.error("EEEERRRRRR", e);
			writer.write(new BaseResponse(ResponseCode.ERROR));
		}
	}

	private class DataWriteTask extends WriteTask<WriteResult[]> {
		private WriteFileMessage message;
		private WriteResult[] results;
		private Pair<RecordFileWriter, WriteWorker> binding;
		private ResponseWriter<BaseResponse> writer;
		
		public DataWriteTask(Pair<RecordFileWriter, WriteWorker> binding, WriteFileMessage message, ResponseWriter<BaseResponse> writer) {
			this.binding = binding;
			this.message = message;
			this.writer = writer;
		}

		@Override
		protected WriteResult[] execute() throws Exception {
			WriteFileData[] datas = message.getDatas();
			
			results = new WriteResult[datas.length];
			
			RecordFileWriter writer = binding.first();
			for(int i = 0; i < datas.length; i++) {
				byte[] contentData = fileFormater.formatData(datas[i].getData());
				
				LOG.debug("writing file[{}] with data size[{}]", writer.getPath(), contentData.length);
				
				WriteResult result = new WriteResult(fileFormater.relativeOffset(writer.position()), contentData.length);
				writer.write(contentData);
				
				writerManager.flushIfNeeded(writer.getPath());
				results[i] = result;
			}
			
			return results;
		}

		@Override
		protected void onPostExecute(WriteResult[] result) {
			try {
				BaseResponse response = new BaseResponse(ResponseCode.OK);
				WriteResultList resultList = new WriteResultList();
				resultList.setWriteResults(result);
				response.setBody(ProtoStuffUtils.serialize(resultList));
				
				writer.write(response);
			} catch (IOException e) {
				LOG.error("onPostExecute error", e);
				writer.write(new BaseResponse(ResponseCode.ERROR));
			}
		}

		@Override
		protected void onFailed(Throwable cause) {
			try {
				BaseResponse response = new BaseResponse(ResponseCode.OK);
				WriteResultList resultList = new WriteResultList();
				resultList.setWriteResults(results);
				response.setBody(ProtoStuffUtils.serialize(resultList));
				
				writer.write(response);
			} catch (IOException e) {
				LOG.error("onFailed error", e);
				writer.write(new BaseResponse(ResponseCode.ERROR));
			}
		}
		
	}
}
