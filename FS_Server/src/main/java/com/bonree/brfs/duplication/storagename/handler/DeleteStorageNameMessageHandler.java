package com.bonree.brfs.duplication.storagename.handler;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bonree.brfs.common.http.HandleResult;
import com.bonree.brfs.common.http.HandleResultCallback;
import com.bonree.brfs.common.service.Service;
import com.bonree.brfs.common.service.ServiceManager;
import com.bonree.brfs.common.utils.BrStringUtils;
import com.bonree.brfs.common.utils.CloseUtils;
import com.bonree.brfs.configuration.ServerConfig;
import com.bonree.brfs.disknode.client.DiskNodeClient;
import com.bonree.brfs.disknode.client.HttpDiskNodeClient;
import com.bonree.brfs.duplication.storagename.StorageNameManager;
import com.bonree.brfs.duplication.storagename.exception.StorageNameNonexistentException;
import com.bonree.brfs.duplication.storagename.exception.StorageNameRemoveException;
import com.bonree.brfs.server.ReturnCode;

public class DeleteStorageNameMessageHandler extends StorageNameMessageHandler {
    
    private static final Logger LOG = LoggerFactory.getLogger(DeleteStorageNameMessageHandler.class);

    private StorageNameManager storageNameManager;

    private ServiceManager serviceManager;

    public DeleteStorageNameMessageHandler(StorageNameManager storageNameManager, ServiceManager serviceManager) {
        this.storageNameManager = storageNameManager;
        this.serviceManager = serviceManager;
    }

    @Override
    public void handleMessage(StorageNameMessage msg, HandleResultCallback callback) {
        // TODO 删除前先检测其是否还有数据关联
        boolean deleted;
        HandleResult result = new HandleResult();
        try {
            deleted = storageNameManager.removeStorageName(msg.getName());
            result.setSuccess(deleted);
            if (!deleted) {
                result.setData(BrStringUtils.toUtf8Bytes("errorCode:224"));
            } else {
               List<Service> services= serviceManager.getServiceListByGroup(ServerConfig.DEFAULT_DISK_NODE_SERVICE_GROUP);
               boolean deleteCompleted = true;
               for(Service service : services) {
                   DiskNodeClient client = null;
                   try {
                       client = new HttpDiskNodeClient(service.getHost(), service.getPort());
                       LOG.info("Deleting----[{}]", "/"+msg.getName());
                       deleteCompleted &= client.deleteDir("/"+msg.getName(), true, true);
                   } catch(Exception e) {
                       e.printStackTrace();
                   } finally {
                       CloseUtils.closeQuietly(client);
                   }
               }
               
               result.setSuccess(deleteCompleted);
            }
        } catch (StorageNameNonexistentException e) {
            result.setSuccess(false);
            result.setData(BrStringUtils.toUtf8Bytes(ReturnCode.STORAGE_NONEXIST_ERROR.codeDetail()));
        } catch (StorageNameRemoveException e) {
            result.setSuccess(false);
            result.setData(BrStringUtils.toUtf8Bytes(ReturnCode.STORAGE_REMOVE_ERROR.codeDetail()));
        }

        callback.completed(result);
    }

}
