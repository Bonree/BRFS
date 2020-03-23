package com.bonree.brfs.duplication.rocksdb.handler;

import com.bonree.brfs.common.net.http.HandleResult;
import com.bonree.brfs.common.net.http.HandleResultCallback;
import com.bonree.brfs.common.net.http.HttpMessage;
import com.bonree.brfs.common.net.http.MessageHandler;
import com.bonree.brfs.common.utils.JsonUtils;
import com.bonree.brfs.common.utils.ZipUtils;
import com.bonree.brfs.configuration.Configs;
import com.bonree.brfs.configuration.units.RocksDBConfigs;
import com.bonree.brfs.rebalance.transfer.SimpleFileClient;
import com.fasterxml.jackson.core.type.TypeReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

/*******************************************************************************
 * 版权信息：北京博睿宏远数据科技股份有限公司
 * Copyright: Copyright (c) 2007博睿宏远科技发展有限公司,Inc.All Rights Reserved.
 *
 * @Date 2020/3/23 15:13
 * @Author: <a href=mailto:zhangqi@bonree.com>张奇</a>
 * @Description:
 ******************************************************************************/
public class EstablishSocketRequestHandler implements MessageHandler {

    private static final Logger LOG = LoggerFactory.getLogger(EstablishSocketRequestHandler.class);

    @Override
    public boolean isValidRequest(HttpMessage message) {
        return true;
    }

    @Override
    public void handle(HttpMessage msg, HandleResultCallback callback) {
        byte[] data = msg.getContent();
        String tmpFileName = msg.getParams().get("tmpFileName");
        String targetBackupPath = msg.getParams().get("backupPath");
        String socketHost = msg.getParams().get("host");
        int socketPort = Integer.parseInt(msg.getParams().get("port"));

        String backupPath = Configs.getConfiguration().GetConfig(RocksDBConfigs.ROCKSDB_BACKUP_PATH);

        try {
            List<String> files = JsonUtils.toObject(data, new TypeReference<List<String>>() {
            });

            List<String> prefixFiles = new ArrayList<>();
            for (String file : files) {
                prefixFiles.add(backupPath + "/" + file);
            }

            String outDir = backupPath + "/" + tmpFileName;
            ZipUtils.zip(prefixFiles, outDir);
            SimpleFileClient client = new SimpleFileClient();
            for (String file : files) {
                client.sendFile(socketHost, socketPort, outDir, targetBackupPath, file);
            }

            if (new File(outDir).delete()) {
                LOG.info("client delete tmp transfer file :{}", outDir);
            }
            callback.completed(new HandleResult());
        } catch (Exception e) {
            LOG.error("establish socket handler err, host:port {}:{}", socketHost, socketPort);
            callback.completed(new HandleResult(false));
        }
    }
}
