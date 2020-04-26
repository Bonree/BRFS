package com.bonree.brfs.disknode.server.tcp.handler;

import com.bonree.brfs.common.net.tcp.BaseMessage;
import com.bonree.brfs.common.net.tcp.BaseResponse;
import com.bonree.brfs.common.net.tcp.MessageHandler;
import com.bonree.brfs.common.net.tcp.ResponseCode;
import com.bonree.brfs.common.net.tcp.ResponseWriter;
import com.bonree.brfs.common.serialize.ProtoStuffUtils;
import com.bonree.brfs.disknode.DiskContext;
import com.bonree.brfs.disknode.data.write.FileWriterManager;
import com.bonree.brfs.disknode.server.tcp.handler.data.DeleteFileMessage;
import java.io.File;
import java.util.LinkedList;
import java.util.Queue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DeleteFileMessageHandler implements MessageHandler<BaseResponse> {
    private static final Logger LOG = LoggerFactory.getLogger(DeleteFileMessageHandler.class);

    private DiskContext diskContext;
    private FileWriterManager writerManager;

    public DeleteFileMessageHandler(DiskContext context, FileWriterManager nodeManager) {
        this.diskContext = context;
        this.writerManager = nodeManager;
    }

    @Override
    public void handleMessage(BaseMessage baseMessage, ResponseWriter<BaseResponse> writer) {
        DeleteFileMessage message = ProtoStuffUtils.deserialize(baseMessage.getBody(), DeleteFileMessage.class);
        if (message == null) {
            writer.write(new BaseResponse(ResponseCode.ERROR_PROTOCOL));
            return;
        }

        try {
            String filePath = diskContext.getConcreteFilePath(message.getFilePath());

            LOG.info("delete file[{}], force[{}], recursive[{}]", filePath, message.isForce(), message.isRecursive());
            File targetFile = new File(filePath);
            if (targetFile.isFile()) {
                try {
                    closeFile(targetFile, message.isForce());

                    writer.write(new BaseResponse(ResponseCode.OK));
                } catch (Exception e) {
                    writer.write(new BaseResponse(ResponseCode.ERROR));
                    return;
                }
            } else {
                try {
                    closeDir(targetFile, message.isRecursive(), message.isForce());
                    writer.write(new BaseResponse(ResponseCode.OK));
                } catch (Exception e) {
                    writer.write(new BaseResponse(ResponseCode.ERROR));
                    return;
                }
            }
        } catch (Exception e) {
            LOG.error("delete message error", e);
            writer.write(new BaseResponse(ResponseCode.ERROR));
        }
    }

    private void closeFile(File file, boolean forceClose) throws IllegalStateException {
        LOG.info("DISK Deleting file[{}]", file.getAbsolutePath());

        if (writerManager.getBinding(file.getAbsolutePath(), false) == null) {
            file.delete();
            return;
        }

        if (!forceClose) {
            throw new IllegalStateException("File[" + file.getAbsolutePath() + "] is under writing now!");
        }

        writerManager.close(file.getAbsolutePath());
        file.delete();
    }

    private void closeDir(File dir, boolean recursive, boolean forceClose) {
        Queue<File> fileQueue = new LinkedList<File>();
        LinkedList<File> deletingDirs = new LinkedList<File>();
        fileQueue.add(dir);

        if (dir.list().length == 0) {
            dir.delete();
            return;
        }

        if (!recursive) {
            throw new IllegalStateException("Directory[" + dir.getAbsolutePath() + "] is not empty!");
        }

        //第一轮先删除普通文件节点
        while (!fileQueue.isEmpty()) {
            File file = fileQueue.poll();
            if (file.isDirectory()) {
                for (File child : file.listFiles()) {
                    fileQueue.add(child);
                }

                deletingDirs.addFirst(file);
            } else {
                closeFile(file, forceClose);
            }
        }

        //第二轮删除文件夹节点
        for (File deleteDir : deletingDirs) {
            LOG.info("DISK Deleting dir[{}]", deleteDir.getAbsolutePath());
            deleteDir.delete();
        }
    }

}
