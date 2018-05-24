package com.bonree.brfs.duplication.storagename.handler;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bonree.brfs.common.ReturnCode;
import com.bonree.brfs.common.ZookeeperPaths;
import com.bonree.brfs.common.http.HandleResult;
import com.bonree.brfs.common.http.HandleResultCallback;
import com.bonree.brfs.common.service.Service;
import com.bonree.brfs.common.service.ServiceManager;
import com.bonree.brfs.common.task.TaskType;
import com.bonree.brfs.common.utils.BrStringUtils;
import com.bonree.brfs.configuration.ServerConfig;
import com.bonree.brfs.duplication.storagename.StorageNameManager;
import com.bonree.brfs.duplication.storagename.StorageNameNode;
import com.bonree.brfs.duplication.storagename.exception.StorageNameNonexistentException;
import com.bonree.brfs.duplication.storagename.exception.StorageNameRemoveException;
import com.bonree.brfs.schedulers.task.TasksUtils;
import com.bonree.brfs.schedulers.task.manager.MetaTaskManagerInterface;
import com.bonree.brfs.schedulers.task.manager.impl.DefaultReleaseTask;
import com.bonree.brfs.schedulers.task.model.TaskModel;
import com.bonree.brfs.schedulers.task.model.TaskServerNodeModel;

public class DeleteStorageNameMessageHandler extends StorageNameMessageHandler {

    private static final Logger LOG = LoggerFactory.getLogger(DeleteStorageNameMessageHandler.class);

    private StorageNameManager storageNameManager;

    private ServiceManager serviceManager;

    private ServerConfig serverConfig;

    private ZookeeperPaths zkPaths;

    public DeleteStorageNameMessageHandler(ServerConfig serverConfig, ZookeeperPaths zkPaths, StorageNameManager storageNameManager, ServiceManager serviceManager) {
        this.serverConfig = serverConfig;
        this.zkPaths = zkPaths;
        this.storageNameManager = storageNameManager;
        this.serviceManager = serviceManager;
    }

    @Override
    public void handleMessage(StorageNameMessage msg, HandleResultCallback callback) {
        boolean deleted;
        HandleResult result = new HandleResult();
        try {
            deleted = storageNameManager.removeStorageName(msg.getName());
            result.setSuccess(deleted);
            if (!deleted) {
                result.setSuccess(false);
                result.setData(BrStringUtils.toUtf8Bytes(ReturnCode.STORAGE_OPT_ERROR.name()));
            } else {
                LOG.info("clean all data for " + msg.getName());

                List<Service> services = serviceManager.getServiceListByGroup(ServerConfig.DEFAULT_DISK_NODE_SERVICE_GROUP);
                StorageNameNode sn = storageNameManager.findStorageName(msg.getName());
                TaskModel task = TasksUtils.createUserDelete(sn, TaskType.USER_DELETE, "", -1, System.currentTimeMillis());
                MetaTaskManagerInterface release = DefaultReleaseTask.getInstance();
                release.setPropreties(serverConfig.getZkHosts(), zkPaths.getBaseTaskPath(), zkPaths.getBaseLocksPath());
                // 创建任务节点
                String taskName = release.updateTaskContentNode(task, TaskType.USER_DELETE.name(), null);
                TaskServerNodeModel serverModel = TasksUtils.createServerTaskNode();
                // 创建服务节点
                for (Service service : services) {
                    release.updateServerTaskContentNode(service.getServiceId(), taskName, TaskType.USER_DELETE.name(), serverModel);
                }

                result.setSuccess(true);
                result.setData(BrStringUtils.toUtf8Bytes(ReturnCode.SUCCESS.name()));
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
