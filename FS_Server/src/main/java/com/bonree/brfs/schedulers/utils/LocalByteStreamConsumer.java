package com.bonree.brfs.schedulers.utils;

import com.bonree.brfs.disknode.client.DiskNodeClient.ByteConsumer;
import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LocalByteStreamConsumer implements ByteConsumer {
    private static final Logger LOG = LoggerFactory.getLogger(LocalByteStreamConsumer.class);
    private CompletableFuture<Boolean> result = new CompletableFuture<>();
    private String localPath;
    private int bufferSize;
    private BufferedOutputStream output = null;
    private ByteArrayOutputStream byteStream = new ByteArrayOutputStream();

    public LocalByteStreamConsumer(String localPath, int bufferSize) {
        this.localPath = localPath;
        this.bufferSize = bufferSize;
    }

    public LocalByteStreamConsumer(String localPath) {
        this(localPath, 5 * 1024 * 1024);
    }

    @Override
    public void consume(byte[] bytes, boolean endOfConsume) {
        try {
            this.byteStream.write(bytes);
        } catch (IOException e) {
            LOG.error("write happen error! data[{}]", new String(bytes), e);
        }
        if (endOfConsume) {
            wirteLocalFileData(this.byteStream.toByteArray());
            result.complete(true);
        }

    }

    public void wirteLocalFileData(byte[] data) {
        File outFile = new File(localPath);
        try {
            File parentDir = outFile.getParentFile();
            if (!parentDir.exists()) {
                FileUtils.forceMkdir(parentDir);
            }
            if (output == null) {

                output = new BufferedOutputStream(new FileOutputStream(new File(localPath)), bufferSize);
            }
            output.write(data);
            output.flush();
        } catch (IOException e) {
            LOG.error("write byte error ", e);
        } finally {
            try {
                if (output != null) {
                    output.close();
                }
            } catch (IOException e) {
                LOG.error("close error ", e);
            }
        }
    }

    @Override
    public void error(Throwable e) {
        LOG.error("recovery file error!! clear data !!! ", e);
        result.completeExceptionally(e);
    }

    public CompletableFuture<Boolean> getResult() {
        return result;
    }

}
