package com.bonree.brfs.disknode.server.handler;

import com.bonree.brfs.common.net.http.HandleResult;
import com.bonree.brfs.common.net.http.HandleResultCallback;
import com.bonree.brfs.common.net.http.HttpMessage;
import com.bonree.brfs.common.net.http.MessageHandler;
import com.bonree.brfs.common.utils.JsonUtils;
import com.bonree.brfs.disknode.DiskContext;
import com.bonree.brfs.disknode.server.handler.data.FileInfo;
import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.LinkedList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ListMessageHandler implements MessageHandler {
    private static final Logger LOG = LoggerFactory.getLogger(ListMessageHandler.class);

    private DiskContext context;

    private LinkedList<FileInfo> fileList = new LinkedList<FileInfo>();

    public ListMessageHandler(DiskContext context) {
        this.context = context;
    }

    @Override
    public void handle(HttpMessage msg, HandleResultCallback callback) {
        HandleResult result = new HandleResult();
        String dirPath = null;
        try {
            dirPath = context.getConcreteFilePath(msg.getPath());
            int level = Integer.parseInt(msg.getParams().getOrDefault("level", "1"));

            File dir = new File(dirPath);
            if (!dir.exists()) {
                result.setSuccess(false);
                result.setCause(new FileNotFoundException(msg.getPath()));
                return;
            }

            if (!dir.isDirectory()) {
                result.setSuccess(false);
                result.setCause(new IllegalAccessException("[" + msg.getPath() + "] is not directory"));
                return;
            }

            FileInfo dirInfo = new FileInfo();
            dirInfo.setLevel(0);
            dirInfo.setType(FileInfo.TYPE_DIR);
            dirInfo.setPath(dirPath);
            fileList.addLast(dirInfo);

            ArrayList<FileInfo> fileInfoList = new ArrayList<FileInfo>();
            traverse(level, fileInfoList);

            result.setSuccess(true);
            result.setData(JsonUtils.toJsonBytes(fileInfoList));
        } catch (Exception e) {
            LOG.error("list dir[{}] error", dirPath, e);
            result.setSuccess(false);
        } finally {
            callback.completed(result);
        }

    }

    private void traverse(int level, ArrayList<FileInfo> fileInfoList) {
        while (!fileList.isEmpty()) {
            FileInfo fileInfo = fileList.remove();

            if (fileInfo.getType() == FileInfo.TYPE_DIR && fileInfo.getLevel() < level) {
                File[] subFiles = new File(fileInfo.getPath()).listFiles();
                if (subFiles != null && subFiles.length > 0) {
                    for (File subFile : subFiles) {
                        FileInfo info = new FileInfo();
                        info.setLevel(fileInfo.getLevel() + 1);
                        info.setType(subFile.isDirectory() ? FileInfo.TYPE_DIR : FileInfo.TYPE_FILE);
                        info.setPath(subFile.getAbsolutePath());
                        fileList.addLast(info);
                    }
                }
            }

            fileInfo.setPath(context.getLogicFilePath(fileInfo.getPath()));
            fileInfoList.add(fileInfo);
        }
    }

    @Override
    public boolean isValidRequest(HttpMessage message) {
        return true;
    }
}
