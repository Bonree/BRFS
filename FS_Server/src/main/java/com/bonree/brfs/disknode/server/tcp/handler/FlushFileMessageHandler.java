package com.bonree.brfs.disknode.server.tcp.handler;

import com.bonree.brfs.common.net.tcp.BaseMessage;
import com.bonree.brfs.common.net.tcp.BaseResponse;
import com.bonree.brfs.common.net.tcp.MessageHandler;
import com.bonree.brfs.common.net.tcp.ResponseCode;
import com.bonree.brfs.common.net.tcp.ResponseWriter;
import com.bonree.brfs.common.utils.BrStringUtils;
import com.bonree.brfs.disknode.DiskContext;
import com.bonree.brfs.disknode.data.write.FileWriterManager;
import java.io.FileNotFoundException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FlushFileMessageHandler implements MessageHandler<BaseResponse> {
    private static final Logger LOG = LoggerFactory.getLogger(FlushFileMessageHandler.class);

    private DiskContext diskContext;
    private FileWriterManager writerManager;

    public FlushFileMessageHandler(DiskContext diskContext, FileWriterManager nodeManager) {
        this.diskContext = diskContext;
        this.writerManager = nodeManager;
    }

    @Override
    public void handleMessage(BaseMessage baseMessage, ResponseWriter<BaseResponse> writer) {
        String path = BrStringUtils.fromUtf8Bytes(baseMessage.getBody());
        if (path == null) {
            writer.write(new BaseResponse(ResponseCode.ERROR_PROTOCOL));
            return;
        }

        String filePath = null;
        try {
            filePath = diskContext.getConcreteFilePath(path);
            LOG.info("flush file[{}]", filePath);
            writerManager.flushFile(filePath);

            writer.write(new BaseResponse(ResponseCode.OK));
        } catch (FileNotFoundException e) {
            writer.write(new BaseResponse(ResponseCode.ERROR));
        }
    }

}
