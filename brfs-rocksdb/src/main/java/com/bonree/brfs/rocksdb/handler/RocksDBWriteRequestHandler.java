package com.bonree.brfs.rocksdb.handler;

import com.bonree.brfs.common.net.http.HandleResult;
import com.bonree.brfs.common.net.http.HandleResultCallback;
import com.bonree.brfs.common.net.http.HttpMessage;
import com.bonree.brfs.common.net.http.MessageHandler;
import com.bonree.brfs.common.serialize.ProtoStuffUtils;
import com.bonree.brfs.rocksdb.RocksDBDataUnit;
import com.bonree.brfs.rocksdb.RocksDBManager;
import com.bonree.brfs.rocksdb.WriteStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*******************************************************************************
 * 版权信息：北京博睿宏远数据科技股份有限公司
 * Copyright: Copyright (c) 2007博睿宏远科技发展有限公司,Inc.All Rights Reserved.
 *
 * @Date 2020/3/19 14:51
 * @Author: <a href=mailto:zhangqi@bonree.com>张奇</a>
 * @Description:
 ******************************************************************************/
public class RocksDBWriteRequestHandler implements MessageHandler {
    private static final Logger LOG = LoggerFactory.getLogger(RocksDBWriteRequestHandler.class);

    private RocksDBManager rocksDBManager;

    public RocksDBWriteRequestHandler(RocksDBManager rocksDBManager) {
        this.rocksDBManager = rocksDBManager;
    }

    @Override
    public boolean isValidRequest(HttpMessage message) {
        return true;
    }

    @Override
    public void handle(HttpMessage msg, HandleResultCallback callback) {
        byte[] data = msg.getContent();
        RocksDBDataUnit dataUnit = ProtoStuffUtils.deserialize(data, RocksDBDataUnit.class);

        HandleResult result = new HandleResult();
        try {
            WriteStatus status = rocksDBManager.write(dataUnit);
            if (status == WriteStatus.SUCCESS) {
                LOG.debug("sync rocksdb data success, data:{}", dataUnit);
                result.setSuccess(true);
            } else {
                LOG.debug("sync rocksdb data failed, data:{}", dataUnit);
                result.setSuccess(false);
            }
            callback.completed(result);
        } catch (Exception e) {
            LOG.error("sync rocksdb data exception, data:{}", dataUnit, e);
            result.setSuccess(false);
            callback.completed(result);
        }
    }
}
