package com.bonree.brfs.disknode.server.tcp.handler;

import com.bonree.brfs.common.net.tcp.BaseMessage;
import com.bonree.brfs.common.net.tcp.BaseResponse;
import com.bonree.brfs.common.net.tcp.MessageHandler;
import com.bonree.brfs.common.net.tcp.ResponseCode;
import com.bonree.brfs.common.net.tcp.ResponseWriter;
import com.bonree.brfs.common.utils.BrStringUtils;
import com.bonree.brfs.common.utils.BufferUtils;
import com.bonree.brfs.common.utils.ByteUtils;
import com.bonree.brfs.common.write.data.FileEncoder;
import com.bonree.brfs.disknode.DiskContext;
import com.bonree.brfs.disknode.data.read.DataFileReader;
import com.bonree.brfs.disknode.data.write.FileWriterManager;
import com.bonree.brfs.disknode.data.write.RecordFileWriter;
import com.bonree.brfs.disknode.data.write.worker.WriteTask;
import com.bonree.brfs.disknode.data.write.worker.WriteWorker;
import com.bonree.brfs.disknode.fileformat.FileFormater;
import com.bonree.brfs.disknode.utils.Pair;
import com.google.common.io.Files;
import com.google.common.primitives.Bytes;
import com.google.common.primitives.Longs;
import java.io.File;
import java.io.IOException;
import java.nio.MappedByteBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CloseFileMessageHandler implements MessageHandler<BaseResponse> {
    private static final Logger LOG = LoggerFactory.getLogger(CloseFileMessageHandler.class);

    private DiskContext diskContext;
    private FileWriterManager writerManager;
    private FileFormater fileFormater;

    public CloseFileMessageHandler(DiskContext context, FileWriterManager nodeManager, FileFormater fileFormater) {
        this.diskContext = context;
        this.writerManager = nodeManager;
        this.fileFormater = fileFormater;
    }

    @Override
    public void handleMessage(BaseMessage baseMessage, ResponseWriter<BaseResponse> writer) {
        String path = BrStringUtils.fromUtf8Bytes(baseMessage.getBody());
        if (path == null) {
            writer.write(new BaseResponse(ResponseCode.ERROR_PROTOCOL));
            return;
        }

        try {
            final String filePath = diskContext.getConcreteFilePath(path);
            LOG.info("CLOSE file[{}]", filePath);

            Pair<RecordFileWriter, WriteWorker> binding = writerManager.getBinding(filePath, false);
            if (binding == null) {
                LOG.info("no writer is found for file[{}], treat it as OK!", filePath);

                File dataFile = new File(filePath);
                if (!dataFile.exists()) {
                    writer.write(new BaseResponse(ResponseCode.ERROR));
                    return;
                }

                MappedByteBuffer buffer = Files.map(dataFile);
                try {
                    buffer.position(fileFormater.fileHeader().length());
                    buffer.limit(buffer.capacity() - fileFormater.fileTailer().length());
                    BaseResponse response = new BaseResponse(ResponseCode.OK);
                    response.setBody(Longs.toByteArray(ByteUtils.crc(buffer)));
                    BufferUtils.release(buffer);
                    writer.write(response);
                    return;
                } finally {
                    BufferUtils.release(buffer);
                }

            }

            binding.second().put(new WriteTask<Long>() {

                @Override
                protected Long execute() throws Exception {
                    LOG.info("start writing file tailer for {}", filePath);
                    binding.first().flush();
                    byte[] fileBytes = DataFileReader.readFile(filePath, fileFormater.fileHeader().length());
                    long crcCode = ByteUtils.crc(fileBytes);
                    LOG.info("final crc code[{}] by bytes[{}] of file[{}]", crcCode, fileBytes.length, filePath);

                    byte[] tailer = Bytes.concat(FileEncoder.validate(crcCode), FileEncoder.tail());

                    binding.first().write(tailer);
                    binding.first().flush();

                    LOG.info("close over for file[{}]", filePath);
                    writerManager.close(filePath);

                    return crcCode;
                }

                @Override
                protected void onPostExecute(Long result) {
                    BaseResponse response = new BaseResponse(ResponseCode.OK);
                    response.setBody(Longs.toByteArray(result));
                    writer.write(response);
                }

                @Override
                protected void onFailed(Throwable e) {
                    BaseResponse response = new BaseResponse(ResponseCode.ERROR);
                    writer.write(response);
                }
            });
        } catch (IOException e) {
            LOG.error("close file[{}] error!", path);
            writer.write(new BaseResponse(ResponseCode.ERROR));
        }
    }

}
