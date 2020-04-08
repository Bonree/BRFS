package com.bonree.brfs.duplication.datastream.blockcache;

import com.bonree.brfs.common.net.http.HandleResult;
import com.bonree.brfs.common.net.http.HandleResultCallback;
import com.bonree.brfs.common.net.http.data.FSPacket;
import com.bonree.brfs.common.utils.JsonUtils;
import com.bonree.brfs.configuration.Configs;
import com.bonree.brfs.configuration.units.RegionNodeConfigs;
import com.bonree.brfs.duplication.datastream.writer.DefaultStorageRegionWriter;
import com.bonree.brfs.duplication.datastream.writer.StorageRegionWriteCallback;
import com.bonree.brfs.duplication.datastream.writer.StorageRegionWriter;
import com.google.common.base.Preconditions;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

public class SeqBlockManager implements BlockManagerInterface{
    private DefaultStorageRegionWriter writer;
    private static final Logger LOG = LoggerFactory.getLogger(SeqBlockManager.class);
    private static long blockSize = Configs.getConfiguration().GetConfig(RegionNodeConfigs.CONFIG_BLOCK_SIZE);

    private LoadingCache<BlockKey,BlockValue> blockcache = CacheBuilder.newBuilder().
            concurrencyLevel(Runtime.getRuntime()
                    .availableProcessors())
            .maximumSize(64)
            .initialCapacity(32)
            .expireAfterAccess(30, TimeUnit.SECONDS)
            .build(new CacheLoader<BlockKey, BlockValue>(){

                @Override
                public BlockValue load(BlockKey blockKey) throws Exception {
                    return new BlockValue(new byte[(int)blockSize]);
                }
            });
    @Inject
    public SeqBlockManager(BlockPool blockPool, StorageRegionWriter writer) {
        this.writer = (DefaultStorageRegionWriter) writer;
    }
    @Override
    public Block appendToBlock(FSPacket packet, HandleResultCallback callback) {
        int storage = packet.getStorageName();
        long packetOffsetInFile = packet.getOffsetInFile();
        long blockOffsetInFile = packet.getBlockOffsetInFile(blockSize);
        long blockSizeInFile;
        long packetSizeInblock;
        String fileName = packet.getFileName();
        try {
            BlockValue blockValue = blockcache.get(new BlockKey(packet.getStorageName(), packet.getFileName()));
            if(packet.getData().length + blockValue.getBlockPos()>blockSize){
                HandleResult result = new HandleResult();
                result.setSuccess(false);
                result.setCause(new Exception("error when append block into block,cause error packet size"));
                LOG.debug("error when append block into block,cause error packet size[{}]",packet.getData().length);
                callback.completed(result);
                return null;
            }
            boolean needflush = blockValue.appendPacket(packet.getData());
            //flush a file
            if(packet.isLastPacketInFile()){
                writer.write(storage,blockValue.getRealData(),
                        new WriteBlockCallback(callback,packet, packet.isLastPacketInFile()));
                LOG.info("flush a block into data pool ");
                return null;
            }
            if(needflush){//flush a block
                writer.write(storage, blockValue.getRealData(),
                        new WriteBlockCallback(callback, packet, packet.isLastPacketInFile()));
                LOG.info("flush a block into data pool ");
                return null;
            }
            HandleResult handleResult = new HandleResult();
            LOG.debug("packet[{}] 追加到block,且未写满block，累积在内存中。。。，",packet);
            handleResult.setNextSeqno(packet.getSeqno());
            handleResult.setCONTINUE();
            callback.completed(handleResult);

        } catch (ExecutionException e) {
            HandleResult result = new HandleResult();
            result.setSuccess(false);
            result.setCause(new Exception("error when get block from cache"));
            LOG.debug("error when get block from cache");
            callback.completed(result);
            e.printStackTrace();
            return null;
        }
        return null;
    }

    @Override
    public long getBlockSize() {
        return 0;
    }
    class BlockKey{
        private int storageID;
        private String fileName;
        public BlockKey(int storageName, String fileName) {
            this.storageID = storageName;
            this.fileName = fileName;
        }

        public int getStorageID() {
            return storageID;
        }

        public void setStorageID(int storageID) {
            this.storageID = storageID;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            BlockKey blockKey = (BlockKey) o;
            return storageID == blockKey.storageID &&
                    fileName.equals(blockKey.fileName);
        }

        @Override
        public int hashCode() {
            return Objects.hash(storageID, fileName);
        }
    }
    class BlockValue{
        private byte[] data;
        private List<String> fids = new ArrayList<String>();
        private int pos = 0;
        public BlockValue(byte[] bytes) {
            this.data = bytes;
        }

        public byte[] getRealData(){
            byte[] realData = new byte[pos];
            System.arraycopy(data,0,realData,0,pos);
            return realData;
        }

        public int getBlockPos(){
            return pos;
        }
        public boolean appendPacket(byte[] pData){
            if(pos+pData.length == blockSize){
                System.arraycopy(pData,0,data,pos,pData.length);
                pos+=pData.length;
                LOG.info("prepare flush a block ...");
                return true;
            }
            System.arraycopy(pData,0,data,pos,pData.length);
            pos+=pData.length;
            return false;
        }
        public byte[] writeFile(int storageName, String fileName) {
            StringBuilder sb = new StringBuilder("::brfs-index-file::storage[" + storageName + "]file[" + fileName + "]\n");
            for (String fid : fids) {
                sb.append(fid).append("\n");
            }
            return sb.toString().getBytes();
        }
        public void reset(){
            pos = 0;
        }

        public void addFid(String fid) {
            fids.add(fid);
        }
    }
    private class WriteBlockCallback implements StorageRegionWriteCallback {
        private HandleResultCallback callback;
        private int storageName;
        private String fileName;
        private Boolean isFileFinished;
        private long blockOffsetInfile;
        private long seqno;
        private Block block;

        /**
         * 用来处理写大文件一个block的结果，如果一个block作为file写入的，那么它应该在writeFileCallback中处理
         * 1. 不是最后一个block
         * 2. 是最后一个block
         * 上面两种情况都触发一个对blockcache的写fid操作
         * 并将阶段性结果返回给客户端
         *
         * @param callback
         * @param packet
         */
        public WriteBlockCallback(HandleResultCallback callback, FSPacket packet,boolean isFileFinished) {
            this.callback = callback;
            this.storageName = packet.getStorageName();
            this.fileName = packet.getFileName();
            this.isFileFinished = isFileFinished;
            this.blockOffsetInfile = packet.getBlockOffsetInFile(blockSize);
            this.seqno = packet.getSeqno();
        }

        @Override
        public void complete(String[] fids) {
            HandleResult result = new HandleResult();
            result.setSuccess(false);
            result.setCause(new Exception("按文件流写入文件不应该一次向datapool中写多个！"));
            LOG.debug("error come here");
            callback.completed(result);
        }

        @Override
        public void complete(String fid) {
            HandleResult result = new HandleResult();
            Preconditions.checkNotNull(fid, "在写block[{}]时返回的fid为空",block);
            // 1. blockcache中填写fid
            try {
                BlockValue blockValue = blockcache.get(new BlockKey(storageName, fileName));
                blockValue.addFid(fid);
                if(!isFileFinished){
                    String response = "seqno:" + seqno +
                            " filename:" + fileName +
                            " storageName:" + storageName +
                            " done flush";
                    LOG.debug(response);
                    result.setCONTINUE();
                    result.setNextSeqno(seqno);
                    callback.completed(result);
                }else{
                    byte[] data = blockValue.writeFile(storageName, fileName);
                    writer.write(storageName,data,
                            new WriteFileCallback(callback,storageName,fileName));
                    LOG.info("在[{}]中写了一个大文件索引文件[{}]", storageName, fileName);
                }

            } catch (ExecutionException e) {
                result.setSuccess(false);
                result.setCause(new Exception("error when append block into block,cause error packet size"));
                LOG.error("error when append block into block in callback");
                callback.completed(result);
                e.printStackTrace();
            }

        }

        @Override
        public void error() {
            LOG.error("文件[{}]seqno:" + seqno +
                    "数据包所在的offset为：" + blockOffsetInfile +
                    "的block" + block +
                    "flush时出错！", fileName);
            callback.completed(new HandleResult(false));
        }
    }
    private class WriteFileCallback implements StorageRegionWriteCallback {

        private HandleResultCallback callback;
        private int StorageName;
        private String fileName;
        public WriteFileCallback(HandleResultCallback callback, int storage,String file) {
            this.callback = callback;
            this.StorageName = storage;
            this.fileName = file;
        }

        @Override
        public void complete(String[] fids) {
            HandleResult result = new HandleResult();
            result.setSuccess(false);
            result.setCause(new Exception("按文件流写入文件不应该一次向datapool中写多个！"));
            LOG.debug("error come here");
            callback.completed(result);
        }

        @Override
        public void complete(String fid) {
            HandleResult result = new HandleResult();

            try {
                LOG.debug("flush一个文件,fid[{}]", fid);
                //todo 写目录树

                result.setData(JsonUtils.toJsonBytes(fid));
                result.setSuccess(true);
            } catch (JsonUtils.JsonException e) {
                LOG.error("can not json fids", e);
                result.setSuccess(false);
            }

            callback.completed(result);
        }

        @Override
        public void error() {
            callback.completed(new HandleResult(false));
        }
    }
}
