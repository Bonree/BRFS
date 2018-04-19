package com.bonree.brfs.duplication.storagename.handler;

import java.util.Map;

import com.bonree.brfs.common.http.HandleResultCallback;
import com.bonree.brfs.common.http.HttpMessage;
import com.bonree.brfs.common.http.MessageHandler;
import com.google.common.base.Splitter;

public abstract class StorageNameMessageHandler implements MessageHandler {

	@Override
	public void handle(HttpMessage msg, HandleResultCallback callback) {
		StorageNameMessage message = new StorageNameMessage();
		message.setName(parseName(msg.getPath()));
		
		System.out.println("name----" + message.getName());
		
		Map<String, String> params = msg.getParams();
		message.setReplicas(Integer.parseInt(params.get("replicas")));
		message.setTtl(Integer.parseInt(params.get("ttl")));
		
		handleMessage(message, callback);
	}
	
	protected abstract void handleMessage(StorageNameMessage msg, HandleResultCallback callback);
	
	private String parseName(String uri) {
		return Splitter.on('/').omitEmptyStrings().trimResults().splitToList(uri).get(0);
	}

}
