package com.bonree.brfs.duplication.storagename.handler;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bonree.brfs.common.http.HandleResultCallback;
import com.bonree.brfs.common.http.HttpMessage;
import com.bonree.brfs.common.http.MessageHandler;
import com.bonree.brfs.duplication.storagename.StorageNameNode;
import com.google.common.base.Splitter;

public abstract class StorageNameMessageHandler implements MessageHandler {
	private static final Logger LOG = LoggerFactory.getLogger(StorageNameMessageHandler.class);
	
	private static final String PARAM_REPLICATION = "replicas";
	private static final String PARAM_TTL = "ttl";

	@Override
	public void handle(HttpMessage msg, HandleResultCallback callback) {
		StorageNameMessage message = new StorageNameMessage();
		message.setName(parseName(msg.getPath()));
		
		LOG.info("handle StorageName[{}]", message.getName());
		
		Map<String, String> params = msg.getParams();
		if(params.containsKey(PARAM_REPLICATION)) {
			message.addAttribute(StorageNameNode.ATTR_REPLICATION, Integer.parseInt(params.get(PARAM_REPLICATION)));
		}
		
		if(params.containsKey(PARAM_TTL)) {
			message.addAttribute(StorageNameNode.ATTR_TTL, Integer.parseInt(params.get(PARAM_TTL)));
		}
		
		handleMessage(message, callback);
	}
	
	protected abstract void handleMessage(StorageNameMessage msg, HandleResultCallback callback);
	
	private String parseName(String uri) {
		return Splitter.on('/').omitEmptyStrings().trimResults().splitToList(uri).get(0);
	}

}
