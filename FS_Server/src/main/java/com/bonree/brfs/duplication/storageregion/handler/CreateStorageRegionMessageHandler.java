package com.bonree.brfs.duplication.storageregion.handler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bonree.brfs.common.net.http.HandleResult;
import com.bonree.brfs.common.net.http.HandleResultCallback;
import com.bonree.brfs.duplication.storageregion.StorageRegion;
import com.bonree.brfs.duplication.storageregion.StorageRegionConfig;
import com.bonree.brfs.duplication.storageregion.StorageRegionManager;

public class CreateStorageRegionMessageHandler extends StorageRegionMessageHandler {
    private static final Logger LOG = LoggerFactory.getLogger(CreateStorageRegionMessageHandler.class);

    private StorageRegionManager storageRegionManager;

    public CreateStorageRegionMessageHandler(StorageRegionManager storageRegionManager) {
        this.storageRegionManager = storageRegionManager;
    }

    @Override
    public void handleMessage(StorageRegionMessage msg, HandleResultCallback callback) {
        LOG.info("create storage region[{}] with attrs {}", msg.getName(), msg.getAttributes());
        
        try {
        	StorageRegionConfig config = new StorageRegionConfig();
        	config.update(msg.getAttributes());
        	
            StorageRegion node = storageRegionManager.createStorageRegion(msg.getName(), config);
            LOG.info("created region[{}]", node);
            
            callback.completed(new HandleResult(true));
        } catch (Exception e) {
            callback.completed(new HandleResult(false));
        }
    }

}
