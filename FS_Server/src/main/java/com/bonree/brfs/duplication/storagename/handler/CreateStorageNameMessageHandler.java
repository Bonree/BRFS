package com.bonree.brfs.duplication.storagename.handler;

import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bonree.brfs.common.ReturnCode;
import com.bonree.brfs.common.net.http.HandleResult;
import com.bonree.brfs.common.net.http.HandleResultCallback;
import com.bonree.brfs.common.utils.Attributes;
import com.bonree.brfs.common.utils.BrStringUtils;
import com.bonree.brfs.duplication.storagename.StorageNameManager;
import com.bonree.brfs.duplication.storagename.StorageNameNode;
import com.bonree.brfs.duplication.storagename.exception.StorageNameExistException;

public class CreateStorageNameMessageHandler extends StorageNameMessageHandler {
    private static final Logger LOG = LoggerFactory.getLogger(CreateStorageNameMessageHandler.class);

    private StorageNameManager storageNameManager;

    public CreateStorageNameMessageHandler(StorageNameManager storageNameManager) {
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

        Attributes atts = msg.getAttributes();
        Set<String> attNames = atts.getAttributeNames();
        for (String name : attNames) {
            if (StorageNameNode.ATTR_REPLICATION.equals(name)) {
                if (atts.getInt(name) <= 0 || atts.getInt(name) > 16) {
                    result.setSuccess(false);
                    result.setData(BrStringUtils.toUtf8Bytes(ReturnCode.STORAGE_REPLICATION_ERROR.name()));
                    callback.completed(result);
                    return;
                }
            } else if (StorageNameNode.ATTR_TTL.equals(name)) {
                if (atts.getInt(name) == 0) {
                    result.setSuccess(false);
                    result.setData(BrStringUtils.toUtf8Bytes(ReturnCode.STORAGE_TTL_ERROR.name()));
                    callback.completed(result);
                    return;
                }
            }
        }

        StorageNameNode node = null;
        try {
            node = storageNameManager.createStorageName(msg.getName(), msg.getAttributes());
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
