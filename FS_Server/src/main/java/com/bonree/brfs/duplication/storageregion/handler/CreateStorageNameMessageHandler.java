package com.bonree.brfs.duplication.storageregion.handler;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bonree.brfs.common.ReturnCode;
import com.bonree.brfs.common.net.http.HandleResult;
import com.bonree.brfs.common.net.http.HandleResultCallback;
import com.bonree.brfs.common.utils.BrStringUtils;
import com.bonree.brfs.duplication.storageregion.StorageRegion;
import com.bonree.brfs.duplication.storageregion.StorageRegionManager;
import com.bonree.brfs.duplication.storageregion.exception.StorageNameExistException;

public class CreateStorageNameMessageHandler extends StorageNameMessageHandler {
    private static final Logger LOG = LoggerFactory.getLogger(CreateStorageNameMessageHandler.class);

    private StorageRegionManager storageNameManager;

    public CreateStorageNameMessageHandler(StorageRegionManager storageNameManager) {
        this.storageNameManager = storageNameManager;
    }

    @Override
    public void handleMessage(StorageNameMessage msg, HandleResultCallback callback) {
        LOG.info("create storageName[{}]", msg.getName(), msg.getAttributes());
        HandleResult result = new HandleResult();

        if (StringUtils.isEmpty(msg.getName()) || msg.getName().length() > 64) {
            result.setSuccess(false);
            result.setData(BrStringUtils.toUtf8Bytes(ReturnCode.STORAGE_NAME_ERROR.name()));
            callback.completed(result);
            return;
        }

        StorageRegion node = null;
        try {
            node = storageNameManager.createStorageRegion(msg.getName(), msg.getAttributes());
            LOG.info("created NODE[{}]", node);

            if (node == null) {
                result.setSuccess(false);
                result.setData(BrStringUtils.toUtf8Bytes(ReturnCode.STORAGE_OPT_ERROR.name()));
            } else {
                result.setSuccess(true);
                // byte[] nodeBytes = null;
                // try {
                // nodeBytes = ProtoStuffUtils.serialize(node);
                // } catch (IOException e) {
                // e.printStackTrace();
                // }
                result.setData(BrStringUtils.toUtf8Bytes(ReturnCode.SUCCESS.name()));
            }
        } catch (StorageNameExistException e) {
            result.setSuccess(false);
            result.setData(BrStringUtils.toUtf8Bytes(ReturnCode.STORAGE_EXIST_ERROR.name()));
        }

        callback.completed(result);
    }

}
