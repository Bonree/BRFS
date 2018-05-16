package com.bonree.brfs.disknode.server.handler;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bonree.brfs.common.http.HandleResult;
import com.bonree.brfs.common.http.HandleResultCallback;
import com.bonree.brfs.common.http.HttpMessage;
import com.bonree.brfs.common.http.MessageHandler;
import com.bonree.brfs.common.utils.JsonUtils;
import com.bonree.brfs.common.utils.ProtoStuffUtils;
import com.bonree.brfs.common.utils.ThreadPoolUtil;
import com.bonree.brfs.disknode.DiskContext;
import com.bonree.brfs.disknode.client.WriteResult;
import com.bonree.brfs.disknode.data.write.FileWriterManager;
import com.bonree.brfs.disknode.data.write.RecordFileWriter;
import com.bonree.brfs.disknode.data.write.worker.WriteTask;
import com.bonree.brfs.disknode.data.write.worker.WriteWorker;
import com.bonree.brfs.disknode.server.handler.data.WriteData;
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
		HandleResult handleResult = new HandleResult();
		
		try {
			String realPath = diskContext.getConcreteFilePath(msg.getPath());
			LOG.debug("WRITE [{}], data length[{}]", realPath, msg.getContent().length);
			
			if(msg.getContent().length == 0) {
				throw new IllegalArgumentException("Writing data is Empty!!");
			}
			
			boolean json = msg.getParams().containsKey("json");
			
			WriteData item = ProtoStuffUtils.deserialize(msg.getContent(), WriteData.class);
			
			LOG.debug("seq[{}], size[{}]", item.getSequence(), item.getBytes().length);
			
			Pair<RecordFileWriter, WriteWorker> binding = writerManager.getBinding(realPath, true);
			if(binding == null) {
				throw new IllegalStateException("File Writer is null");
			}
			
			binding.second().put(new WriteTask<DataWriteResult>() {

				@Override
				protected DataWriteResult execute() throws IOException {
					RecordFileWriter writer = binding.first();
					DataWriteResult result = new DataWriteResult();
					
					writer.updateSequence(item.getSequence());
					
					result.setOffset(writer.position());
					writer.write(item.getBytes());
					
					result.setSize(item.getBytes().length);
					return result;
				}

				@Override
				protected void onPostExecute(DataWriteResult result) {
					ThreadPoolUtil.commonPool().execute(new Runnable() {
						
						@Override
						public void run() {
							handleResult.setSuccess(true);
							
							WriteResult writeResult = new WriteResult();
							writeResult.setOffset(result.getOffset());
							writeResult.setSize(result.getSize());
							try {
								handleResult.setData(json ? JsonUtils.toJsonBytes(writeResult) : ProtoStuffUtils.serialize(writeResult));
							} catch (Exception e) {
								e.printStackTrace();
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
							handleResult.setSuccess(false);
							handleResult.setCause(e);
							
							callback.completed(handleResult);
						}
					});
				}
			});
		} catch (Exception e) {
			LOG.error("EEEERRRRRR", e);
			handleResult.setSuccess(false);
			handleResult.setCause(e);
			callback.completed(handleResult);
		}
	}

	private class DataWriteResult {
		private long offset;
		private int size;

		public long getOffset() {
			return offset;
		}

		public void setOffset(long offset) {
			this.offset = offset;
		}

		public int getSize() {
			return size;
		}

		public void setSize(int size) {
			this.size = size;
		}
		
		@Override
		public String toString() {
			StringBuilder builder = new StringBuilder();
			builder.append("[offset=").append(offset)
			       .append(", size=").append(size)
			       .append("]");
			
			return builder.toString();
		}
	}
}
