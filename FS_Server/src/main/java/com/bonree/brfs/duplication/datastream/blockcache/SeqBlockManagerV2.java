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
import com.bonree.brfs.common.rocksdb.RocksDBManager;
import com.google.common.base.Preconditions;
import com.google.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class SeqBlockManagerV2 implements BlockManagerInterface{
    private StorageRegionWriter writer;
    private RocksDBManager rocksDBManager;
    private static final Logger LOG = LoggerFactory.getLogger(SeqBlockManagerV2.class);
    private static long blockSize = Configs.getConfiguration().GetConfig(RegionNodeConfigs.CONFIG_BLOCK_SIZE);
    private static int blockPoolSize = Configs.getConfiguration().GetConfig(RegionNodeConfigs.CONFIG_BLOCK_POOL_CAPACITY);
    private static long initBlockSize = 1024 * 1024;
    private LinkedBlockingQueue<WriteFileRequest> fileWaiting = new LinkedBlockingQueue(100);
    private AtomicInteger fileWritingCount = new AtomicInteger(0);
    private ExecutorService fileWorker;
    private final AtomicBoolean runningState = new AtomicBoolean(false);
    private volatile boolean quit = false;
    SeqBlockPool blockPool = new SeqBlockPool(blockSize,blockPoolSize,1);
    private ExecutorService blockManageWatcher = this.fileWorker = new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS,
            new LinkedBlockingQueue<Runnable>(),
            new PooledThreadFactory("watcher:"));

    private ConcurrentHashMap<BlockKey,BlockValue> blockcache = new ConcurrentHashMap<>();
    @Inject
    public SeqBlockManagerV2(BlockPool blockPool, StorageRegionWriter writer) {
        this.writer = writer;
        this.fileWorker = new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<Runnable>(),
                new PooledThreadFactory("blockManager_"+ SeqBlockManagerV2.class.getSimpleName()));

        this.fileWorker.execute(new FileProcessor());
        this.blockManageWatcher.execute(new WatcherProcessor());
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
                LOG.error("can not get block value of file[{}] in blockcache",fileName);
                callback.completed(result);
                return null;
            }
            if(packet.getData().length + blockValue.getBlockPos()>blockSize){
                HandleResult result = new HandleResult();
                result.setSuccess(false);
                result.setCause(new Exception("error when append block into block,cause error packet size"));
                LOG.error("error when append block into block,cause error packet size[{}]",packet.getData().length);
                callback.completed(result);
                return null;
            }
            boolean needflush = blockValue.appendPacket(packet.getData());
            //flush a file
            if(packet.isLastPacketInFile()){
                if(packet.getBlockOffsetInFile(blockSize)==0){
                    writer.write(storage,blockValue.getRealData(),
                            new WriteFileCallback(callback,storage,fileName,false));
                    LOG.info("flushing a small file[{}] into the data pool",fileName);
                    blockValue.releaseData();
                    blockcache.remove(new BlockKey(storage,fileName));
                    return null;
                }else {
                    // we should flush the last block to get its fid
                    writer.write(storage,blockValue.getRealData(),
                            new WriteBlockCallback(callback,packet,true));
                    LOG.info("flushing the last block of file [{}] into the data pool ",fileName);
                    blockValue.releaseData();
                    return null;
                }
            }
            if(needflush){//flush a block
                writer.write(storage, blockValue.getRealData(),
                        new WriteBlockCallback(callback, packet, packet.isLastPacketInFile()));
                LOG.info("flush a block of file[{}] into data pool ",fileName);
                blockValue.reset();
                //todo 这里应该在block返回fid后才可以申请下一个  加return
                return null;
            }
            if(packet.isTheFirstPacketInFile()){
                LOG.info("response for the next packet of this file :seqno [{}]",packet.getSeqno());
            }
            if(packet.isLastPacketInFile()){
                LOG.info("the last packet of file [{}] has arrived",fileName);
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
        private int storage;
        private String file;
        private volatile long accessTime;
        private volatile long createTime;
        private volatile int clearTimeOut = Configs.getConfiguration().GetConfig(RegionNodeConfigs.CLEAR_TIME_THRESHOLD);
        ClearTimerTask fooTimerTask = new ClearTimerTask(clearTimeOut);
        private Timer timer = new Timer();
        public BlockValue(SeqBlock block,int storage,String file) {
            this.data = block;
            accessTime = System.currentTimeMillis();
            createTime = accessTime;
            this.storage = storage;
            this.file = file;
            timer.schedule(fooTimerTask, clearTimeOut, clearTimeOut);
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
        boolean isPutBack(){
            return data ==null;
        }
        public byte[] writeFile(int storageName, String fileName) {
            StringBuilder sb = new StringBuilder("::brfs-index-file::storage[" + storageName + "]file[" + fileName + "]\n");
            for (String fid : fids) {
                sb.append(fid).append("\n");
            }
            accessTime = System.currentTimeMillis();
            LOG.info("file[{}] : fids :\n[{}]",fileName,sb);
            return sb.toString().getBytes();
        }
        public void reset(){
            data.reset();
        }
        public void releaseData(){
            data.reset();
            blockPool.putbackBlocks(data);
            fileWritingCount.decrementAndGet();
            data = null;
            LOG.info("decrement fileWritingCount , now its size is [{}]",fileWritingCount.get());
        }
        public void addFid(String fid) {
            fids.add(fid);
            accessTime = System.currentTimeMillis();
        }

        class ClearTimerTask extends TimerTask {
            long timeout;
            public ClearTimerTask(long timeout) {
                this.timeout = timeout;
            }
            @Override
            public void run() {
                if(System.currentTimeMillis()-accessTime> timeout){
                    LOG.info("clear a file [{}] out of blockcache.",file);
                    // 3. clear file on heap
                    if(!isPutBack())releaseData();
                    BlockValue remove = blockcache.remove(new BlockKey(storage, file));
                    if(remove == null ){
                        cancel();
                        return;
                    }

                }
            }
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
            LOG.error("error come here");
            callback.completed(result);
        }

        @Override
        public void complete(String fid) {
            HandleResult result = new HandleResult();
            if(fid == ""|| fid == null){
                BlockValue blockValue = blockcache.remove(new BlockKey(storageName,fileName));
                if(blockValue != null && fileWritingCount.get()>0){
                    blockValue.releaseData();
                }
                result.setSuccess(false);
                LOG.error("dataengine return a null fid of "+isFileFinished+" file :[{}] on flush a block stage",fileName);
                result.setCause(new BRFSException("flush file or block error"));
                callback.completed(result);
                return;
            }
            // 1. blockcache中填写fid
            try {
                BlockValue blockValue = blockcache.get(new BlockKey(storageName, fileName));
                if(blockValue == null){
                    result.setSuccess(false);
                    LOG.error("flush file or block error,cause there is no [{}] in blockcache",fileName);
                    result.setCause(new BRFSException("flush file or block error,cause there is no element in blockcache"));
                    callback.completed(result);
                    return;
                }
                blockValue.addFid(fid);
                if(!isFileFinished){
                    String response = "seqno:" + seqno +
                            " filename:" + fileName +
                            " storageName:" + storageName +
                            " done flush";
                    // DONE flush a block
                    result.setCONTINUE();
                    result.setNextSeqno(seqno);
                    callback.completed(result);
                    LOG.info(response);
                }else{
                    byte[] data = blockValue.writeFile(storageName, fileName);
                    writer.write(storageName,data,
                            new WriteFileCallback(callback,storageName,fileName,true));
//                    blockValue.releaseData();

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
        private int storageName;
        private String fileName;
        private boolean isBigFile;
        public WriteFileCallback(HandleResultCallback callback, int storage, String file, boolean b) {
            this.callback = callback;
            this.storageName = storage;
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
            LOG.info("prepare response a fid [{}]!",fid);
            HandleResult result = new HandleResult();
            if(fid == ""|| fid == null){
                result.setSuccess(false);
                LOG.error("dataengine return a null fid of  :[{}] on flush a file stage",fileName);
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
        private long cTime ;
        private FSPacket fsPacket;
        private int waitTimeOut = Configs.getConfiguration().GetConfig(RegionNodeConfigs.FILE_WAIT_FOR_WRITE_TIME);
        private HandleResultCallback handleResultCallback;

        public WriteFileRequest(FSPacket fsPacket, HandleResultCallback handleResultCallback) {
            this.fsPacket = fsPacket;
            this.handleResultCallback = handleResultCallback;
            cTime = System.currentTimeMillis();
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
        public boolean ifRequestIsTimeOut(){
            return System.currentTimeMillis() - cTime > waitTimeOut;
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

                if(fileWritingCount.get() < blockPoolSize && fileWaiting.peek() != null){
                    try {

                        LOG.info("Processor : the waiting request size is [{}]",fileWritingCount.get());
                        unhandledRequest = fileWaiting.take();
                        if(unhandledRequest.ifRequestIsTimeOut()){
                            LOG.info("abandon a file write request because of Time out");
                            HandleResult result = new HandleResult();
                            result.setSuccess(false);
                            result.setCause(new Exception("abandon a file write request because of Time out"));
                            unhandledRequest.handleResultCallback.completed(result);
                            continue;
                        }
                        BlockKey blockKey = new BlockKey(unhandledRequest.getFsPacket().getStorageName(),unhandledRequest.getFsPacket().getFileName());
                        SeqBlock block = blockPool.getBlock();
                        blockcache.put(blockKey,new BlockValue(block,unhandledRequest.getFsPacket().getStorageName(),unhandledRequest.getFsPacket().getFileName()));
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
    private class WatcherProcessor implements Runnable {
        @Override
        public void run() {

            while(true) {
                if(quit && fileWaiting.isEmpty()) {
                    break;
                }
                LOG.info("Watcher : enqueue file count is [{}]" +
                                "file writing count is [{}]" +
                                "file in blockcache is [{}]" ,
                        fileWaiting.size(),
                        fileWritingCount.get(),
                        blockcache.size());
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }

            LOG.info("Write file worker is shut down!");
        }

    }
}
