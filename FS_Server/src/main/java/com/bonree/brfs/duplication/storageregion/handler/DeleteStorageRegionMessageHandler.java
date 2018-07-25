package com.bonree.brfs.duplication.storageregion.handler;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bonree.brfs.common.ReturnCode;
import com.bonree.brfs.common.ZookeeperPaths;
import com.bonree.brfs.common.net.http.HandleResult;
import com.bonree.brfs.common.net.http.HandleResultCallback;
import com.bonree.brfs.common.service.Service;
import com.bonree.brfs.common.service.ServiceManager;
import com.bonree.brfs.common.utils.BrStringUtils;
import com.bonree.brfs.configuration.Configs;
import com.bonree.brfs.configuration.units.CommonConfigs;
import com.bonree.brfs.duplication.storageregion.StorageRegion;
import com.bonree.brfs.duplication.storageregion.StorageRegionManager;
import com.bonree.brfs.duplication.storageregion.exception.StorageNameNonexistentException;
import com.bonree.brfs.schedulers.task.TasksUtils;

public class DeleteStorageRegionMessageHandler extends StorageRegionMessageHandler {

    private static final Logger LOG = LoggerFactory.getLogger(DeleteStorageRegionMessageHandler.class);

    private StorageRegionManager storageRegionManager;

    private ServiceManager serviceManager;

    private ZookeeperPaths zkPaths;

    public DeleteStorageRegionMessageHandler(ZookeeperPaths zkPaths, StorageRegionManager storageRegionManager, ServiceManager serviceManager) {
        this.zkPaths = zkPaths;
        this.storageRegionManager = storageRegionManager;
        this.serviceManager = serviceManager;
    }

    @Override
    public void handleMessage(StorageRegionMessage msg, HandleResultCallback callback) {
    	StorageRegion region = storageRegionManager.findStorageRegionByName(msg.getName());
    	if(region == null) {
    		callback.completed(new HandleResult(false));
    		return;
    	}
    	
        boolean deleted;
        HandleResult result = new HandleResult();
        try {
			List<Service> services = serviceManager.getServiceListByGroup(Configs.getConfiguration().GetConfig(CommonConfigs.CONFIG_DATA_SERVICE_GROUP_NAME));
			if(region.isEnable()){
				result.setSuccess(false);
				result.setData(BrStringUtils.toUtf8Bytes(ReturnCode.STORAGE_REMOVE_ERROR.name()));
				callback.completed(result);
				return;
			}
			ReturnCode code = TasksUtils.createUserDeleteTask(services, zkPaths, region, -1,	System.currentTimeMillis());
			if (!ReturnCode.SUCCESS.equals(code)) {
				result.setSuccess(false);
				result.setData(BrStringUtils.toUtf8Bytes(code.name()));
			} else {
				deleted = storageRegionManager.removeStorageRegion(msg.getName());
				result.setSuccess(deleted);
				if (deleted) {
					LOG.info("clean all data for " + msg.getName());
				}else{
					result.setData(BrStringUtils.toUtf8Bytes(ReturnCode.STORAGE_REMOVE_ERROR.name()));
				}
			}

        } catch (StorageNameNonexistentException e) {
            result.setSuccess(false);
            result.setData(BrStringUtils.toUtf8Bytes(ReturnCode.STORAGE_NONEXIST_ERROR.name()));
        } catch (Exception e) {
            result.setSuccess(false);
            result.setData(BrStringUtils.toUtf8Bytes(ReturnCode.STORAGE_REMOVE_ERROR.name()));
        }
        callback.completed(result);
    }
}
