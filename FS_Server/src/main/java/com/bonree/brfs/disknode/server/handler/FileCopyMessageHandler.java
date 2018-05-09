package com.bonree.brfs.disknode.server.handler;

import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bonree.brfs.common.http.HandleResult;
import com.bonree.brfs.common.http.HandleResultCallback;
import com.bonree.brfs.common.http.HttpMessage;
import com.bonree.brfs.common.http.MessageHandler;
import com.bonree.brfs.common.utils.CloseUtils;
import com.bonree.brfs.common.utils.ProtoStuffUtils;
import com.bonree.brfs.disknode.DiskContext;
import com.bonree.brfs.disknode.client.DiskNodeClient;
import com.bonree.brfs.disknode.client.HttpDiskNodeClient;
import com.bonree.brfs.disknode.data.read.DataFileReader;
import com.bonree.brfs.disknode.server.handler.data.FileCopyMessage;

public class FileCopyMessageHandler implements MessageHandler {
	private static final Logger LOG = LoggerFactory.getLogger(FileCopyMessageHandler.class);
	
	private static final int FROM_BUFFER_SIZE = 64 * 1024;
	private static final int TO_BUFFER_SIZE = 10 * 1024 * 1024;
	
	private DiskContext context;
	
	public FileCopyMessageHandler(DiskContext context) {
		this.context = context;
	}

	@Override
	public void handle(HttpMessage msg, HandleResultCallback callback) {
		HandleResult result = new HandleResult();
		
		FileCopyMessage copyMessage = ProtoStuffUtils.deserialize(msg.getContent(), FileCopyMessage.class);
		String localPath = context.getConcreteFilePath(copyMessage.getLocalPath());
		
		if(copyMessage.getDirect() == FileCopyMessage.DIRECT_FROM_REMOTE) {
			LOG.info("copy from remote[{}] to local[{}]", copyMessage.getRemotePath(), copyMessage.getLocalPath());
			DiskNodeClient client = null;
			BufferedOutputStream output = null;
			try {
				client = new HttpDiskNodeClient(copyMessage.getRemoteHost(), copyMessage.getRemotePort());
				byte[] bytes = client.readData(copyMessage.getRemotePath(), 0, Integer.MAX_VALUE);
				output = new BufferedOutputStream(new FileOutputStream(localPath), FROM_BUFFER_SIZE);
				output.write(bytes);
				output.flush();
				
				result.setSuccess(true);
				callback.completed(result);
			} catch (IOException e) {
				result.setSuccess(false);
				result.setCause(e);
				callback.completed(result);
			} finally {
				CloseUtils.closeQuietly(client);
				CloseUtils.closeQuietly(output);
			}
		} else if(copyMessage.getDirect() == FileCopyMessage.DIRECT_TO_REMOTE) {
			LOG.info("copy from local[{}] to remote[{}]", copyMessage.getLocalPath(), copyMessage.getRemotePath());
			DiskNodeClient client = null;
			try {
				client = new HttpDiskNodeClient(copyMessage.getRemoteHost(), copyMessage.getRemotePort());
				
				byte[] buf;
				int offset = 0;
				while((buf = DataFileReader.readFile(localPath, offset, TO_BUFFER_SIZE)) != null) {
					client.writeData(copyMessage.getRemotePath(), offset, buf);
					offset += TO_BUFFER_SIZE;
				}
				
				client.closeFile(copyMessage.getRemotePath());
				
				result.setSuccess(true);
				callback.completed(result);
			} catch (IOException e) {
				e.printStackTrace();
				result.setSuccess(false);
				result.setCause(e);
				callback.completed(result);
			} finally {
				CloseUtils.closeQuietly(client);
			}
		} else {
			LOG.error("Illegal copy directing!");
			result.setSuccess(false);
			result.setCause(new IllegalArgumentException("unkown direct[" + copyMessage.getDirect() + "]"));
			callback.completed(result);
		}
	}

}
