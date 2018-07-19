package com.bonree.brfs.duplication.storageregion.handler;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bonree.brfs.common.net.http.HandleResultCallback;
import com.bonree.brfs.common.net.http.HttpMessage;
import com.bonree.brfs.common.net.http.MessageHandler;
import com.google.common.base.Splitter;

public abstract class StorageNameMessageHandler implements MessageHandler {
	private static final Logger LOG = LoggerFactory.getLogger(StorageNameMessageHandler.class);
	
	private static final String PARAM_REPLICATION = "replicas";
	private static final String PARAM_TTL = "ttl";
	private static final String PARAM_ENABLE = "enable";
	private static final String PARAM_FILE_CAPACITY = "fileCapacity";
	private static final String PARAM_FILE_PARTITION = "filePatitionDuration";

	@Override
	public void handle(HttpMessage msg, HandleResultCallback callback) {
		StorageNameMessage message = new StorageNameMessage();
		message.setName(parseName(msg.getPath()));
		
		LOG.info("handle StorageName[{}]", message.getName());
		
		Map<String, String> params = msg.getParams();
		LOG.info("params = {}", params);
		if(params.containsKey(PARAM_REPLICATION)) {
			message.addAttribute(StorageNameMessage.ATTR_REPLICATION, Integer.parseInt(params.get(PARAM_REPLICATION)));
		}
		
		if(params.containsKey(PARAM_TTL)) {
			message.addAttribute(StorageNameMessage.ATTR_TTL, params.get(PARAM_TTL));
		}
		
		if(params.containsKey(PARAM_ENABLE)) {
			message.addAttribute(StorageNameMessage.ATTR_ENABLE, Boolean.parseBoolean(params.get(PARAM_ENABLE)));
		}
		
		if(params.containsKey(PARAM_FILE_CAPACITY)) {
			message.addAttribute(StorageNameMessage.ATTR_FILE_CAPACITY, Long.parseLong(params.get(PARAM_FILE_CAPACITY)));
		}
		
		if(params.containsKey(PARAM_FILE_PARTITION)) {
			message.addAttribute(StorageNameMessage.ATTR_FILE_PATITION_DURATION, params.get(PARAM_FILE_PARTITION));
		}
		
		handleMessage(message, callback);
	}
	
	protected abstract void handleMessage(StorageNameMessage msg, HandleResultCallback callback);
	
	private String parseName(String uri) {
		return Splitter.on('/').omitEmptyStrings().trimResults().splitToList(uri).get(0);
	}

	@Override
	public boolean isValidRequest(HttpMessage message) {
		return !parseName(message.getPath()).isEmpty();
	}

}
