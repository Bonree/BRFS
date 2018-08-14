package com.bonree.brfs.disknode.server.tcp.handler;

import java.util.concurrent.ExecutorService;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bonree.brfs.common.filesync.FileObjectSyncState;
import com.bonree.brfs.common.filesync.SyncStateCodec;
import com.bonree.brfs.common.net.tcp.BaseMessage;
import com.bonree.brfs.common.net.tcp.BaseResponse;
import com.bonree.brfs.common.net.tcp.HandleCallback;
import com.bonree.brfs.common.net.tcp.MessageHandler;
import com.bonree.brfs.common.net.tcp.ResponseCode;
import com.bonree.brfs.common.serialize.ProtoStuffUtils;
import com.bonree.brfs.common.service.Service;
import com.bonree.brfs.common.service.ServiceManager;
import com.bonree.brfs.common.utils.CloseUtils;
import com.bonree.brfs.common.write.data.FileDecoder;
import com.bonree.brfs.disknode.DiskContext;
import com.bonree.brfs.disknode.client.DiskNodeClient;
import com.bonree.brfs.disknode.client.HttpDiskNodeClient;
import com.bonree.brfs.disknode.data.write.FileWriterManager;
import com.bonree.brfs.disknode.data.write.RecordFileWriter;
import com.bonree.brfs.disknode.data.write.worker.WriteWorker;
import com.bonree.brfs.disknode.fileformat.FileFormater;
import com.bonree.brfs.disknode.server.tcp.handler.data.FileRecoveryMessage;
import com.bonree.brfs.disknode.utils.Pair;

public class FileRecoveryMessageHandler implements MessageHandler {
	private static final Logger LOG = LoggerFactory.getLogger(FileRecoveryMessageHandler.class);
	
	private DiskContext context;
	private ServiceManager serviceManager;
	private FileWriterManager writerManager;
	private FileFormater fileFormater;
	
	private ExecutorService threadPool;
	
	public FileRecoveryMessageHandler(DiskContext context,
			ServiceManager serviceManager,
			FileWriterManager writerManager,
			FileFormater fileFormater,
			ExecutorService threadPool) {
		this.context = context;
		this.serviceManager = serviceManager;
		this.writerManager = writerManager;
		this.fileFormater = fileFormater;
		this.threadPool = threadPool;
	}

	@Override
	public void handleMessage(BaseMessage baseMessage, HandleCallback callback) {
		FileRecoveryMessage message = ProtoStuffUtils.deserialize(baseMessage.getBody(), FileRecoveryMessage.class);
		if(message == null) {
			callback.complete(new BaseResponse(baseMessage.getToken(), ResponseCode.ERROR_PROTOCOL));
			return;
		}
		
		threadPool.submit(new Runnable() {
			
			@Override
			public void run() {
				String filePath = null;
				try {
					filePath = context.getConcreteFilePath(message.getFilePath());
					LOG.info("starting recover file[{}]", filePath);
					
					Pair<RecordFileWriter, WriteWorker> binding = writerManager.getBinding(filePath, false);
					if(binding == null) {
						callback.complete(new BaseResponse(baseMessage.getToken(), ResponseCode.ERROR));
						return;
					}
					
					binding.first().position(fileFormater.absoluteOffset(message.getOffset()));
					byte[] bytes = null;
					for(String stateString : message.getSources()) {
						FileObjectSyncState state = SyncStateCodec.fromString(stateString);
						Service service = serviceManager.getServiceById(state.getServiceGroup(), state.getServiceId());
						if(service == null) {
							LOG.error("can not get service with[{}:{}]", state.getServiceGroup(), state.getServiceId());
							continue;
						}
						
						DiskNodeClient client = null;
						try {
							LOG.info("get data from{} to recover...", service);
							client = new HttpDiskNodeClient(service.getHost(), service.getPort());
							
							long lackBytes = state.getFileLength() - message.getOffset();
							bytes = client.readData(state.getFilePath(), message.getOffset(), (int) lackBytes);
							if(bytes != null) {
								LOG.info("read bytes length[{}], require[{}]", bytes.length, lackBytes);
								break;
							}
						} catch (Exception e) {
							LOG.error("recover file[{}] error", filePath, e);
						} finally {
							CloseUtils.closeQuietly(client);
						}
					}
					
					if(bytes == null) {
						callback.complete(new BaseResponse(baseMessage.getToken(), ResponseCode.ERROR));
						return;
					}
					
					int offset = 0;
					int size = 0;
					while((size = FileDecoder.getOffsets(offset, bytes)) > 0) {
						LOG.info("rewrite data[offset={}, size={}] to file[{}]", offset, size, filePath);
						binding.first().write(bytes, offset, size);
						offset += size;
						size = 0;
					}
					
					if(offset != bytes.length) {
						LOG.error("perhaps datas that being recoverd is not correct! get [{}], but recoverd[{}]", bytes.length, offset);
					}
					
					callback.complete(new BaseResponse(baseMessage.getToken(), ResponseCode.OK));
				} catch (Exception e) {
					LOG.error("recover file[{}] error", filePath, e);
					callback.complete(new BaseResponse(baseMessage.getToken(), ResponseCode.ERROR));
				}
			}
		});
	}

}
