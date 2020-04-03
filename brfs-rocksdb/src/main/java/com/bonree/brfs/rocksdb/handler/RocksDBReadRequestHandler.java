package com.bonree.brfs.rocksdb.handler;

import com.bonree.brfs.common.net.http.HandleResult;
import com.bonree.brfs.common.net.http.HandleResultCallback;
import com.bonree.brfs.common.net.http.HttpMessage;
import com.bonree.brfs.common.net.http.MessageHandler;
import com.bonree.brfs.rocksdb.RocksDBManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*******************************************************************************
 * 版权信息：北京博睿宏远数据科技股份有限公司
 * Copyright: Copyright (c) 2007博睿宏远科技发展有限公司,Inc.All Rights Reserved.
 *
 * @Date 2020/3/19 14:52
 * @Author: <a href=mailto:zhangqi@bonree.com>张奇</a>
 * @Description:
 ******************************************************************************/
public class RocksDBReadRequestHandler implements MessageHandler {

    private static final Logger LOG = LoggerFactory.getLogger(RocksDBReadRequestHandler.class);

    private RocksDBManager rocksDBManager;

    public RocksDBReadRequestHandler(RocksDBManager rocksDBManager) {
        this.rocksDBManager = rocksDBManager;
    }

    @Override
    public boolean isValidRequest(HttpMessage message) {
        return true;
    }

    @Override
    public void handle(HttpMessage msg, HandleResultCallback callback) {
        String columnFamily = msg.getParams().get("cf");
        String key = msg.getParams().get("key");
        HandleResult result = new HandleResult();
        try {
            byte[] data = this.rocksDBManager.read(columnFamily, key.getBytes(), false);
            result.setData(data);
        } catch (Exception e) {
            LOG.error("rocksdb read request handler err, cf:{}, key:{}", columnFamily, key);
            result.setSuccess(false);
        } finally {
            callback.completed(result);
        }
    }
}
