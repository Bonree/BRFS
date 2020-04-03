package com.bonree.brfs.rocksdb.handler;

import com.bonree.brfs.common.net.http.HandleResult;
import com.bonree.brfs.common.net.http.HandleResultCallback;
import com.bonree.brfs.common.net.http.HttpMessage;
import com.bonree.brfs.common.net.http.MessageHandler;
import com.bonree.brfs.common.utils.FileUtils;
import com.bonree.brfs.common.utils.JsonUtils;
import com.bonree.brfs.common.utils.ZipUtils;
import com.bonree.brfs.configuration.Configs;
import com.bonree.brfs.configuration.units.RocksDBConfigs;
import com.bonree.brfs.rocksdb.backup.RocksDBBackupEngine;
import com.bonree.brfs.rocksdb.file.SimpleFileSender;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.List;

/*******************************************************************************
 * 版权信息：北京博睿宏远数据科技股份有限公司
 * Copyright: Copyright (c) 2007博睿宏远科技发展有限公司,Inc.All Rights Reserved.
 *
 * @Date 2020/3/23 15:13
 * @Author: <a href=mailto:zhangqi@bonree.com>张奇</a>
 * @Description:
 ******************************************************************************/
public class RocksDBRestoreRequestHandler implements MessageHandler {

    private static final Logger LOG = LoggerFactory.getLogger(RocksDBRestoreRequestHandler.class);

    private RocksDBBackupEngine engine;

    public RocksDBRestoreRequestHandler(RocksDBBackupEngine engine) {
        this.engine = engine;
    }

    @Override
    public boolean isValidRequest(HttpMessage message) {
        return true;
    }

    @Override
    public void handle(HttpMessage msg, HandleResultCallback callback) {
        String transferFileName = msg.getParams().get("transferFileName");
        String restorePath = msg.getParams().get("restorePath");
        String host = msg.getParams().get("host");
        int port = Integer.parseInt(msg.getParams().get("port"));

        String backupPath = Configs.getConfiguration().GetConfig(RocksDBConfigs.ROCKSDB_BACKUP_PATH);

        try {
            int backupId = this.engine.createNewBackup();

            // 将备份文件进行压缩后传输到目标节点
            String outDir = backupPath + File.separator + transferFileName;
            ZipUtils.zip(FileUtils.listFilePaths(backupPath), outDir);
            SimpleFileSender sender = new SimpleFileSender();
            sender.send(host, port, outDir, restorePath);

            if (FileUtils.deleteFile(outDir)) {
                LOG.info("socket client delete tmp transfer file :{}", outDir);
            }

            HandleResult result = new HandleResult();
            List<Integer> backupIds = this.engine.getBackupIds();
            result.setData(JsonUtils.toJsonBytes(backupIds));
            LOG.info("restore handler create new backup, this backupId:{}, all backupIds:{}", backupId, backupIds);
            callback.completed(result);
        } catch (Exception e) {
            LOG.error("restore request handler err, host:port {}:{}", host, port);
            callback.completed(new HandleResult(false));
        }
    }
}
