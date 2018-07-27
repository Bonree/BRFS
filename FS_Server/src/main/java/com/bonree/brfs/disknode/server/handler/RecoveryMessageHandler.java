package com.bonree.brfs.disknode.server.handler;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bonree.brfs.common.filesync.FileObjectSyncState;
import com.bonree.brfs.common.filesync.SyncStateCodec;
import com.bonree.brfs.common.net.http.HandleResult;
import com.bonree.brfs.common.net.http.HandleResultCallback;
import com.bonree.brfs.common.net.http.HttpMessage;
import com.bonree.brfs.common.net.http.MessageHandler;
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
import com.bonree.brfs.disknode.utils.Pair;
import com.google.common.base.Splitter;

public class RecoveryMessageHandler implements MessageHandler {
	private static final Logger LOG = LoggerFactory.getLogger(RecoveryMessageHandler.class);
	
	private DiskContext context;
	private ServiceManager serviceManager;
	private FileWriterManager writerManager;
	private FileFormater fileFormater;
	
	public RecoveryMessageHandler(DiskContext context,
			ServiceManager serviceManager,
			FileWriterManager writerManager,
			FileFormater fileFormater) {
		this.context = context;
		this.serviceManager = serviceManager;
		this.writerManager = writerManager;
		this.fileFormater = fileFormater;
	}

	@Override
	public void handle(HttpMessage msg, HandleResultCallback callback) {
		HandleResult handleResult = new HandleResult();
		String filePath = null;
		try {
			filePath = context.getConcreteFilePath(msg.getPath());
			LOG.info("starting recover file[{}]", filePath);
			String lengthParam = msg.getParams().get("length");
			if(lengthParam == null) {
				handleResult.setSuccess(false);
				callback.completed(handleResult);
				return;
			}
			
			long fileLength = Long.parseLong(msg.getParams().get("length"));
			List<String> fullStates = Splitter.on(',').omitEmptyStrings().trimResults().splitToList(msg.getParams().get("fulls"));
			Pair<RecordFileWriter, WriteWorker> binding = writerManager.getBinding(filePath, false);
			if(binding == null) {
				handleResult.setSuccess(false);
				callback.completed(handleResult);
				return;
			}
			
			binding.first().position(fileFormater.absoluteOffset(fileLength));
			byte[] bytes = null;
			for(String stateString : fullStates) {
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
					
					long lackBytes = state.getFileLength() - fileLength;
					bytes = client.readData(state.getFilePath(), fileLength, (int) lackBytes);
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
				handleResult.setSuccess(false);
				callback.completed(handleResult);
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
			
			handleResult.setSuccess(true);
		} catch (Exception e) {
			LOG.error("recover file[{}] error", filePath, e);
			handleResult.setSuccess(false);
		} finally {
			callback.completed(handleResult);
		}
	}
	
	@Override
	public boolean isValidRequest(HttpMessage message) {
		return true;
	}
	
}
