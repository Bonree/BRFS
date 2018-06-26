package com.bonree.brfs.duplication.storagename.handler;

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
import com.bonree.brfs.configuration.units.DiskNodeConfigs;
import com.bonree.brfs.duplication.storagename.StorageNameManager;
import com.bonree.brfs.duplication.storagename.StorageNameNode;
import com.bonree.brfs.duplication.storagename.exception.StorageNameNonexistentException;
import com.bonree.brfs.duplication.storagename.exception.StorageNameRemoveException;
import com.bonree.brfs.schedulers.task.TasksUtils;

public class DeleteStorageNameMessageHandler extends StorageNameMessageHandler {

    private static final Logger LOG = LoggerFactory.getLogger(DeleteStorageNameMessageHandler.class);

    private StorageNameManager storageNameManager;

    private ServiceManager serviceManager;

    private ZookeeperPaths zkPaths;

    public DeleteStorageNameMessageHandler(ZookeeperPaths zkPaths, StorageNameManager storageNameManager, ServiceManager serviceManager) {
        this.zkPaths = zkPaths;
        this.storageNameManager = storageNameManager;
        this.serviceManager = serviceManager;
    }

    @Override
    public void handleMessage(StorageNameMessage msg, HandleResultCallback callback) {
        boolean deleted;
        HandleResult result = new HandleResult();
        try {
			List<Service> services = serviceManager.getServiceListByGroup(Configs.getConfiguration().GetConfig(DiskNodeConfigs.CONFIG_SERVICE_GROUP_NAME));
			StorageNameNode sn = storageNameManager.findStorageName(msg.getName());
			if(sn.isEnable()){
				result.setSuccess(false);
				result.setData(BrStringUtils.toUtf8Bytes(ReturnCode.STORAGE_REMOVE_ERROR.name()));
				callback.completed(result);
				return;
			}
			ReturnCode code = TasksUtils.createUserDeleteTask(services, zkPaths, sn, -1,	System.currentTimeMillis());
			if (!ReturnCode.SUCCESS.equals(code)) {
				result.setSuccess(false);
				result.setData(BrStringUtils.toUtf8Bytes(code.name()));
			} else {
				deleted = storageNameManager.removeStorageName(msg.getName());
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
        } catch (StorageNameRemoveException e) {
            result.setSuccess(false);
            result.setData(BrStringUtils.toUtf8Bytes(ReturnCode.STORAGE_REMOVE_ERROR.name()));
        }
        callback.completed(result);
    }
}
