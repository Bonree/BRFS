package com.bonree.brfs.duplication.datastream.handler;


import com.bonree.brfs.common.net.http.HandleResult;
import com.bonree.brfs.common.net.http.HandleResultCallback;
import com.bonree.brfs.common.net.http.HttpMessage;
import com.bonree.brfs.common.net.http.MessageHandler;
import com.bonree.brfs.common.net.http.data.FSPacket;
import com.bonree.brfs.common.net.http.data.FSPacketUtil;
import com.bonree.brfs.common.utils.JsonUtils;
import com.bonree.brfs.duplication.datastream.blockcache.Block;
import com.bonree.brfs.duplication.datastream.blockcache.BlockManager;
import com.bonree.brfs.duplication.datastream.writer.StorageRegionWriteCallback;
import com.bonree.brfs.duplication.datastream.writer.StorageRegionWriter;
import com.google.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author wangchao
 * @date 2020/3/18 - 5:52 下午
 */
public class WriteStreamDataMessageHandler implements MessageHandler {
    private static final Logger LOG = LoggerFactory.getLogger(WriteStreamDataMessageHandler.class);
    private StorageRegionWriter writer;
    private BlockManager blockManager;
    @Inject
    public WriteStreamDataMessageHandler(StorageRegionWriter writer, BlockManager blockManager) {
        this.writer = writer;
        this.blockManager = blockManager;
    }

    @Override
    public void handle(HttpMessage msg, HandleResultCallback callback) {
        LOG.debug("DONE decode ,从请求中取出data");
        byte[] content = msg.getContent();
        LOG.debug("收到数据长度为：[{}]，尝试将其填充到block中，",content.length);
        try {
            FSPacket packet = FSPacketUtil.deserialize(content);
            int storage = packet.getStorageName();
            String file = packet.getFileName();
            LOG.debug("从数据中反序列化packet [{}]",packet);
            //如果是一个小于等于packet长度的文件，由handler直接写
            if(packet.isATinyFile()){
                writer.write(storage,packet.getData(),new DataWriteCallback(callback));
                LOG.debug("在[{}]的datapool中写了一个小于packetSize的小文件[{}]", storage, file);
                return;
            }

            //===== 追加数据的到blockManager
            Block block = blockManager.appendToBlock(packet, callback);
//            handleResult.setCONTINUE();
//            callback.completed(handleResult);
        } catch (Exception e) {
            LOG.error("handle write data message error", e);
            callback.completed(new HandleResult(false));
        }

    }

    private class DataWriteCallback implements StorageRegionWriteCallback {
        private HandleResultCallback callback;

        /**
         * 小文件的唯一的一packet数据。文件只有一个packet
         * @param callback
         */
        public DataWriteCallback(HandleResultCallback callback) {
            this.callback = callback;
        }

        @Override
        public void complete(String[] fid) {
            HandleResult result = new HandleResult();

            try {
                result.setData(JsonUtils.toJsonBytes(fid));
                result.setSuccess(true);
            } catch (JsonUtils.JsonException e) {
                LOG.error("can not json fids", e);
                result.setSuccess(false);
            }

            callback.completed(result);
        }

        @Override
        public void complete(String fid) {
            return;
        }

        @Override
        public void error() {
            callback.completed(new HandleResult(false));
        }

    }
    @Override
    public boolean isValidRequest(HttpMessage message) {
        if(message.getContent()==null)return false;
        return true;
    }
}
