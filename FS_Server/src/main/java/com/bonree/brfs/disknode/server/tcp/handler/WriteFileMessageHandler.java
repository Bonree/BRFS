package com.bonree.brfs.disknode.server.tcp.handler;

import com.bonree.brfs.common.net.tcp.BaseMessage;
import com.bonree.brfs.common.net.tcp.BaseResponse;
import com.bonree.brfs.common.net.tcp.MessageHandler;
import com.bonree.brfs.common.net.tcp.ResponseCode;
import com.bonree.brfs.common.net.tcp.ResponseWriter;
import com.bonree.brfs.common.serialize.ProtoStuffUtils;
import com.bonree.brfs.disknode.DiskContext;
import com.bonree.brfs.disknode.client.WriteResult;
import com.bonree.brfs.disknode.client.WriteResultList;
import com.bonree.brfs.disknode.data.write.FileWriterManager;
import com.bonree.brfs.disknode.data.write.RecordFileWriter;
import com.bonree.brfs.disknode.data.write.worker.WriteTask;
import com.bonree.brfs.disknode.data.write.worker.WriteWorker;
import com.bonree.brfs.disknode.fileformat.FileFormater;
import com.bonree.brfs.disknode.server.tcp.handler.data.WriteFileData;
import com.bonree.brfs.disknode.server.tcp.handler.data.WriteFileMessage;
import com.bonree.brfs.disknode.utils.Pair;
import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WriteFileMessageHandler implements MessageHandler<BaseResponse> {
    private static final Logger LOG = LoggerFactory.getLogger(WriteFileMessageHandler.class);

    private DiskContext diskContext;
    private FileWriterManager writerManager;
    private FileFormater fileFormater;

    public WriteFileMessageHandler(DiskContext diskContext, FileWriterManager nodeManager, FileFormater fileFormater) {
        this.diskContext = diskContext;
        this.writerManager = nodeManager;
        this.fileFormater = fileFormater;
    }

    @Override
    public void handleMessage(BaseMessage baseMessage, ResponseWriter<BaseResponse> writer) {
        WriteFileMessage message = ProtoStuffUtils.deserialize(baseMessage.getBody(), WriteFileMessage.class);
        if (message == null) {
            writer.write(new BaseResponse(ResponseCode.ERROR_PROTOCOL));
            return;
        }

        try {
            String realPath = diskContext.getConcreteFilePath(message.getFilePath());
            LOG.debug("writing to file [{}]", realPath);

            Pair<RecordFileWriter, WriteWorker> binding = writerManager.getBinding(realPath, false);
            if (binding == null) {
                //运行到这，可能时打开文件时失败，导致写数据节点找不到writer
                LOG.warn("no file writer is found, maybe the file[{}] is closed.", realPath);
                writer.write(new BaseResponse(ResponseCode.ALREADY_CLOSE));
                return;
            }

            binding.second().put(new DataWriteTask(binding, message.getFilePath(), message.getDatas(), writer));
        } catch (Exception e) {
            LOG.error("EEEERRRRRR", e);
            writer.write(new BaseResponse(ResponseCode.ERROR));
        }
    }

    private class DataWriteTask extends WriteTask<WriteResult[]> {
        private final String filePath;
        private final WriteFileData[] datas;
        private final WriteResult[] results;
        private final Pair<RecordFileWriter, WriteWorker> binding;
        private final ResponseWriter<BaseResponse> writer;

        public DataWriteTask(Pair<RecordFileWriter, WriteWorker> binding,
                             String filePath,
                             WriteFileData[] datas,
                             ResponseWriter<BaseResponse> writer) {
            this.binding = binding;
            this.filePath = filePath;
            this.datas = datas;
            this.results = new WriteResult[datas.length];
            this.writer = writer;
        }

        @Override
        protected WriteResult[] execute() throws Exception {
            RecordFileWriter writer = binding.first();

            LOG.info("write [{}] datas to file[{}]", datas.length, writer.getPath());
            for (int i = 0; i < datas.length; i++) {
                byte[] contentData = fileFormater.formatData(datas[i].getData());
                datas[i] = null;

                LOG.debug("writing file[{}] with data size[{}]", writer.getPath(), contentData.length);

                WriteResult result = new WriteResult(fileFormater.relativeOffset(writer.position()), contentData.length);
                LOG.info("write file[{}] in result[{}, {}]", writer.getPath(), result.getOffset(), result.getSize());
                writer.write(contentData);

                writerManager.flushIfNeeded(writer.getPath());
                results[i] = result;
            }

            return results;
        }

        @Override
        protected void onPostExecute(WriteResult[] result) {
            try {
                BaseResponse response = new BaseResponse(ResponseCode.OK);
                WriteResultList resultList = new WriteResultList();
                resultList.setWriteResults(result);
                response.setBody(ProtoStuffUtils.serialize(resultList));

                writer.write(response);
            } catch (IOException e) {
                LOG.error("onPostExecute error", e);
                writer.write(new BaseResponse(ResponseCode.ERROR));
            }
        }

        @Override
        protected void onFailed(Throwable cause) {
            LOG.error("write datas to file[{}] error", filePath, cause);

            try {
                BaseResponse response = new BaseResponse(ResponseCode.OK);
                WriteResultList resultList = new WriteResultList();
                resultList.setWriteResults(results);
                response.setBody(ProtoStuffUtils.serialize(resultList));

                writer.write(response);
            } catch (IOException e) {
                LOG.error("onFailed error", e);
                writer.write(new BaseResponse(ResponseCode.ERROR));
            }
        }

    }
}
