package com.bonree.brfs.duplication.datastream.blockcache;

import com.bonree.brfs.client.BRFSException;
import com.bonree.brfs.common.net.http.HandleResult;
import com.bonree.brfs.common.net.http.HandleResultCallback;
import com.bonree.brfs.common.net.http.data.FSPacket;
import com.bonree.brfs.common.utils.JsonUtils;
import com.bonree.brfs.common.utils.PooledThreadFactory;
import com.bonree.brfs.configuration.Configs;
import com.bonree.brfs.configuration.units.RegionNodeConfigs;
import com.bonree.brfs.duplication.FidBuilder;
import com.bonree.brfs.duplication.datastream.writer.StorageRegionWriteCallback;
import com.bonree.brfs.duplication.datastream.writer.StorageRegionWriter;
import com.bonree.brfs.rocksdb.RocksDBManager;
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
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class SeqBlockManagerV2 implements BlockManagerInterface{
    private StorageRegionWriter writer;
    private RocksDBManager rocksDBManager;
    private static final Logger LOG = LoggerFactory.getLogger(SeqBlockManagerV2.class);
    private static long blockSize = Configs.getConfiguration().GetConfig(RegionNodeConfigs.CONFIG_BLOCK_SIZE);
    private static long initBlockSize = 1024 * 1024;
    private LinkedBlockingQueue<WriteFileRequest> fileWaiting = new LinkedBlockingQueue(100);
    private AtomicInteger fileWritingCount = new AtomicInteger(0);
    private ExecutorService fileWorker;
    private final AtomicBoolean runningState = new AtomicBoolean(false);
    private volatile boolean quit = false;
    SeqBlockPool blockPool = new SeqBlockPool(blockSize,20,1);

    private ConcurrentHashMap<BlockKey,BlockValue> blockcache = new ConcurrentHashMap<>();
    @Inject
    public SeqBlockManagerV2(BlockPool blockPool, StorageRegionWriter writer) {
        this.writer = writer;
        this.fileWorker = new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<Runnable>(),
                new PooledThreadFactory("blockManager_"+ SeqBlockManagerV2.class.getSimpleName()));

        this.fileWorker.execute(new FileProcessor());
    }
    @Inject
    public SeqBlockManagerV2(BlockPool blockPool, StorageRegionWriter writer, RocksDBManager rocksDBManager) {
        this.writer = writer;
        this.rocksDBManager = rocksDBManager;
    }

    @Override
    public void addToWaitingPool(FSPacket packet, HandleResultCallback callback) {
        try {
            fileWaiting.put(new WriteFileRequest(packet,callback));
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        LOG.info("waiting pool size is [{}]",fileWaiting.size());
        LOG.info("storage [{}] : file[{}] waiting for write.",packet.getStorageName(),packet.getFileName());
    }

    @Override
    public Block appendToBlock(FSPacket packet, HandleResultCallback callback) {
        int storage = packet.getStorageName();
        long packetOffsetInFile = packet.getOffsetInFile();
        long blockOffsetInFile = packet.getBlockOffsetInFile(blockSize);
        String fileName = packet.getFileName();
        // file waiting for write
        if(packet.isTheFirstPacketInFile()){
            LOG.info("After processor : storage [{}] : file[{}] waiting for write.",storage,fileName);
        }
        LOG.debug("writing the file [{}]",fileName);
        try {
            BlockValue blockValue = blockcache.get(new BlockKey(packet.getStorageName(), packet.getFileName()));
            if(blockValue == null){
                HandleResult result = new HandleResult();
                result.setSuccess(false);
                result.setCause(new Exception("error when append block into block,cause by long time no write"));
                LOG.debug("error when append block into block,cause error packet size[{}]",packet.getData().length);
                callback.completed(result);
                return null;
            }
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
                if(packet.getBlockOffsetInFile(blockSize)==0){
                    writer.write(storage,blockValue.getRealData(),
                            new WriteFileCallback(callback,storage,fileName,false));
                    LOG.info("flush a small file into the data pool");
                    blockValue.releaseData();
                    return null;
                }else {
                    // we should flush the last block to get its fid
                    writer.write(storage,blockValue.getRealData(),
                            new WriteBlockCallback(callback,packet,true));
                    LOG.info("flush the last block into the data pool ");
                    blockValue.releaseData();
                    return null;
                }
            }
            if(needflush){//flush a block
                writer.write(storage, blockValue.getRealData(),
                        new WriteBlockCallback(callback, packet, packet.isLastPacketInFile()));
                LOG.info("flush a block into data pool ");
                blockValue.reset();
            }
            if(packet.isTheFirstPacketInFile()){
                LOG.info("response for the next packet of this file :seqno [{}]",packet.getSeqno());
            }
            HandleResult handleResult = new HandleResult();
            LOG.debug("packet[{}] append to block and still not flushed。",packet);
            handleResult.setNextSeqno(packet.getSeqno());
            handleResult.setCONTINUE();
            callback.completed(handleResult);

        } catch (Exception e) {
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
        private SeqBlock data;
        private List<String> fids = new ArrayList<String>();
        private volatile long accessTime;
        public BlockValue(SeqBlock block) {
            this.data = block;
            accessTime = System.currentTimeMillis();
        }

        public byte[] getRealData(){
            return data.getRealData();
        }
        public int getBlockPos(){
            return data.getDataOffsetInBlock();
        }
        public boolean appendPacket(byte[] pData){
            data.appendPacket(pData);
            accessTime = System.currentTimeMillis();
            return data.isBlockSpill();
        }

        public byte[] writeFile(int storageName, String fileName) {
            StringBuilder sb = new StringBuilder("::brfs-index-file::storage[" + storageName + "]file[" + fileName + "]\n");
            for (String fid : fids) {
                sb.append(fid).append("\n");
            }
            accessTime = System.currentTimeMillis();
            return sb.toString().getBytes();
        }
        public void reset(){
            data.reset();
        }
        public void releaseData(){
            data.reset();
            blockPool.putbackBlocks(data);
            fileWritingCount.decrementAndGet();
            LOG.info("decrement fileWritingCount , now its size is [{}]",fileWritingCount.get());
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
            if(fid == ""|| fid == null){
                result.setSuccess(false);
                result.setCause(new BRFSException("flush file or block error"));
                callback.completed(result);
                return;
            }
            // 1. blockcache中填写fid
            try {
                BlockValue blockValue = blockcache.get(new BlockKey(storageName, fileName));
                blockValue.addFid(fid);
                if(!isFileFinished){
                    String response = "seqno:" + seqno +
                            " filename:" + fileName +
                            " storageName:" + storageName +
                            " done flush";
                    // DONE flush a block
                    LOG.info(response);
                }else{
                    byte[] data = blockValue.writeFile(storageName, fileName);
                    //todo blockcache recycle
                    writer.write(storageName,data,
                            new WriteFileCallback(callback,storageName,fileName,true));
                    LOG.info("flushing a big file in [{}],the filename is [{}]", storageName, fileName);
                }

            } catch (Exception e) {
                result.setSuccess(false);
                result.setCause(new Exception("error when append block into block,cause error packet size"));
                LOG.error("error when append block into block in callback");
                callback.completed(result);
                e.printStackTrace();
            }

        }

        @Override
        public void error() {
            LOG.error("file[{}]seqno:" + seqno +
                    " blockOffsetInfile：" + blockOffsetInfile +
                    " block" + block +
                    " flush error！", fileName);
            callback.completed(new HandleResult(false));
        }
    }
    private class WriteFileCallback implements StorageRegionWriteCallback {

        private HandleResultCallback callback;
        private int StorageName;
        private String fileName;
        private boolean isBigFile;
        public WriteFileCallback(HandleResultCallback callback, int storage, String file, boolean b) {
            this.callback = callback;
            this.StorageName = storage;
            this.fileName = file;
            this.isBigFile = b;
        }

        @Override
        public void complete(String[] fids) {
            HandleResult result = new HandleResult();
            result.setSuccess(false);
            result.setCause(new BRFSException("brfs wrong usage : we can not flush more than 1 file when write a stream！"));
            LOG.error("error come here");
            callback.completed(result);
        }

        @Override
        public void complete(String fid) {
            HandleResult result = new HandleResult();
            if(fid == ""|| fid == null){
                result.setSuccess(false);
                result.setCause(new BRFSException("flush file or block error"));
                callback.completed(result);
                return;
            }
            try {
                Preconditions.checkNotNull(fid);
                if(isBigFile){
                   fid =  FidBuilder.setFileType(fid);
                }
                LOG.debug("flushed a file,fid[{}]", fid);
                //todo 写目录树

                result.setData(JsonUtils.toJsonBytes(fid));
                result.setSuccess(true);
            } catch (JsonUtils.JsonException e) {
                LOG.error("can not json fids", e);
                result.setSuccess(false);
                result.setCause(e);
            } catch (BRFSException e) {
                LOG.error("can not set big file flag decode fids", e);
                result.setSuccess(false);
                result.setCause(e);
            }

            callback.completed(result);
        }

        @Override
        public void error() {
            callback.completed(new HandleResult(false));
        }
    }

    class WriteFileRequest{
        private FSPacket fsPacket;
        private HandleResultCallback handleResultCallback;

        public WriteFileRequest(FSPacket fsPacket, HandleResultCallback handleResultCallback) {
            this.fsPacket = fsPacket;
            this.handleResultCallback = handleResultCallback;
        }

        public FSPacket getFsPacket() {
            return fsPacket;
        }

        public void setFsPacket(FSPacket fsPacket) {
            this.fsPacket = fsPacket;
        }

        public HandleResultCallback getHandleResultCallback() {
            return handleResultCallback;
        }

        public void setHandleResultCallback(HandleResultCallback handleResultCallback) {
            this.handleResultCallback = handleResultCallback;
        }
    }

    private class FileProcessor implements Runnable {
        @Override
        public void run() {
            if(!runningState.compareAndSet(false, true)) {
                LOG.error("can not execute write file worker again, because it's started!", new IllegalStateException("Write file worker has been started!"));
                return;
            }
            LOG.info("start process waiting request!");
            WriteFileRequest unhandledRequest = null;

            while(true) {
                if(quit && fileWaiting.isEmpty()) {
                    break;
                }

                if(fileWritingCount.get() < 20 && fileWaiting.peek() != null){
                    try {

                        LOG.info("Processor : the waiting request size is [{}]",fileWritingCount.get());
                        unhandledRequest = fileWaiting.take();
                        BlockKey blockKey = new BlockKey(unhandledRequest.fsPacket.getStorageName(), unhandledRequest.fsPacket.getFileName());
                        SeqBlock block = blockPool.getBlock();
                        blockcache.put(blockKey,new BlockValue(block));
                        LOG.info("Processor : writing file [{}]",unhandledRequest.fsPacket.getFileName());
                        fileWritingCount.incrementAndGet();
                        appendToBlock(unhandledRequest.getFsPacket(),unhandledRequest.getHandleResultCallback());
                    } catch (InterruptedException e) {
                        LOG.error("data consumer interrupted.");
                    } catch (Exception e) {
                        LOG.error("process data error", e);
                    }
                }

            }

            LOG.info("Write file worker is shut down!");
        }
    }
}
