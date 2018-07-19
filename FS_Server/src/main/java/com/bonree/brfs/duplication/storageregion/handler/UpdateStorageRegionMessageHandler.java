package com.bonree.brfs.duplication.storageregion.handler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bonree.brfs.common.net.http.HandleResult;
import com.bonree.brfs.common.net.http.HandleResultCallback;
import com.bonree.brfs.duplication.storageregion.StorageRegion;
import com.bonree.brfs.duplication.storageregion.StorageRegionConfig;
import com.bonree.brfs.duplication.storageregion.StorageRegionManager;

public class UpdateStorageRegionMessageHandler extends StorageRegionMessageHandler {
    private static final Logger LOG = LoggerFactory.getLogger(UpdateStorageRegionMessageHandler.class);

    private StorageRegionManager storageRegionManager;

    public UpdateStorageRegionMessageHandler(StorageRegionManager storageRegionManager) {
        this.storageRegionManager = storageRegionManager;
    }

    @Override
    public void handleMessage(StorageRegionMessage msg, HandleResultCallback callback) {
        LOG.info("update storage region[{}] with attrs {}", msg.getName(), msg.getAttributes());
        
        StorageRegion region = storageRegionManager.findStorageRegionByName(msg.getName());
        if(region == null) {
        	callback.completed(new HandleResult(false));
        	return;
        }

        try {
        	StorageRegionConfig config = new StorageRegionConfig(region);
        	config.update(msg.getAttributes());
        	
            storageRegionManager.updateStorageRegion(msg.getName(), config);
            callback.completed(new HandleResult(true));
        } catch (Exception e) {
        	LOG.error("remove nonexist storage region");
            callback.completed(new HandleResult(false));
        }
    }

}
