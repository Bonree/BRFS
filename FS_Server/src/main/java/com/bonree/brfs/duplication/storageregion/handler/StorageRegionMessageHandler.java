package com.bonree.brfs.duplication.storageregion.handler;

import java.time.Duration;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bonree.brfs.common.net.http.HandleResultCallback;
import com.bonree.brfs.common.net.http.HttpMessage;
import com.bonree.brfs.common.net.http.MessageHandler;
import com.bonree.brfs.duplication.storageregion.StorageRegionConfig;
import com.google.common.base.Splitter;

public abstract class StorageRegionMessageHandler implements MessageHandler {
	private static final Logger LOG = LoggerFactory.getLogger(StorageRegionMessageHandler.class);
	
	private static final int DEFAULT_MAX_STORAGE_NAME_LENGTH = 64;

	@Override
	public void handle(HttpMessage msg, HandleResultCallback callback) {
		StorageRegionMessage message = new StorageRegionMessage();
		String name = parseName(msg.getPath());
		LOG.info("handle storage region[{}]", name);
		
		message.setName(name);
		parseAttributes(message, msg.getParams());
		
		handleMessage(message, callback);
	}
	
	protected abstract void handleMessage(StorageRegionMessage msg, HandleResultCallback callback);
	
	private String parseName(String uri) {
		return Splitter.on('/').omitEmptyStrings().trimResults().splitToList(uri).get(0);
	}
	
	private void parseAttributes(StorageRegionMessage message, Map<String, String> params) {
		String enableParam = params.get(StorageRegionConfig.CONFIG_ENABLE);
		if(enableParam != null) {
			message.addAttribute(StorageRegionConfig.CONFIG_ENABLE, Boolean.parseBoolean(enableParam));
		}
		
		String replicateNumParam = params.get(StorageRegionConfig.CONFIG_REPLICATE_NUM);
		if(replicateNumParam != null) {
			try {
				message.addAttribute(StorageRegionConfig.CONFIG_REPLICATE_NUM, Integer.parseInt(replicateNumParam));
			} catch (Exception e) {}
			
		}
		
		String ttlParam = params.get(StorageRegionConfig.CONFIG_DATA_TTL);
		if(ttlParam != null) {
			try {
				Duration.parse(ttlParam);
				message.addAttribute(StorageRegionConfig.CONFIG_DATA_TTL, ttlParam);
			} catch (Exception e) {}
		}
		
		String capacityParam = params.get(StorageRegionConfig.CONFIG_FILE_CAPACITY);
		if(capacityParam != null) {
			try {
				message.addAttribute(StorageRegionConfig.CONFIG_FILE_CAPACITY, Long.parseLong(capacityParam));
			} catch (Exception e) {}
		}
		
		String partitionDurationParam = params.get(StorageRegionConfig.CONFIG_FILE_PARTITION_DURATION);
		if(partitionDurationParam != null) {
			try {
				Duration.parse(partitionDurationParam);
				message.addAttribute(StorageRegionConfig.CONFIG_FILE_PARTITION_DURATION, partitionDurationParam);
			} catch (Exception e) {}
		}
	}

	@Override
	public boolean isValidRequest(HttpMessage message) {
		String regionName = parseName(message.getPath());
		if(regionName.isEmpty() || regionName.length() > DEFAULT_MAX_STORAGE_NAME_LENGTH) {
			return false;
		}
		
		return true;
	}

}
