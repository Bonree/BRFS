package com.bonree.brfs.duplication.datastream.blockcache;

import com.bonree.brfs.client.BRFSException;
import com.bonree.brfs.common.net.http.HandleResult;
import com.bonree.brfs.common.net.http.HandleResultCallback;
import com.bonree.brfs.common.net.http.data.FSPacket;
import com.bonree.brfs.common.utils.PooledThreadFactory;
import com.bonree.brfs.configuration.Configs;
import com.bonree.brfs.configuration.units.RegionNodeConfigs;
import com.bonree.brfs.duplication.FidBuilder;
import com.bonree.brfs.duplication.datastream.writer.StorageRegionWriteCallback;
import com.bonree.brfs.duplication.datastream.writer.StorageRegionWriter;
import com.google.common.base.Preconditions;
import com.google.inject.Inject;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SeqBlockManagerV2 implements BlockManager {
    private final BlockPool blockPool;
    private StorageRegionWriter writer;
    private static final Logger LOG = LoggerFactory.getLogger(SeqBlockManagerV2.class);
    private static long blockSize = Configs.getConfiguration().getConfig(RegionNodeConfigs.CONFIG_BLOCK_SIZE);
    private static int blockPoolSize = Configs.getConfiguration().getConfig(RegionNodeConfigs.CONFIG_BLOCK_POOL_CAPACITY);
    private LinkedBlockingQueue<WriteRequest> fileWaiting;
    private static Lock LOCK = new ReentrantLock();
    private Condition allowWrite = LOCK.newCondition();
    private AtomicInteger fileWritingCount = new AtomicInteger(0);
    private final AtomicBoolean runningState = new AtomicBoolean(false);
    private volatile boolean quit = false;
    // 避免信号丢失的问题
    private volatile boolean wasSignalled = false;

    private ConcurrentHashMap<BlockKey, BlockValue> blockcache = new ConcurrentHashMap<>();

    @Inject
    public SeqBlockManagerV2(BlockPool blockPool, StorageRegionWriter writer) {
        this.fileWaiting = new LinkedBlockingQueue();
        this.writer = writer;
        ExecutorService fileWorker = new ThreadPoolExecutor(2,
                                                            2,
                                                            0L,
                                                            TimeUnit.MILLISECONDS,
                                                            new LinkedBlockingQueue<>(),
                                                            new PooledThreadFactory("write-request-worker"));
        fileWorker.execute(new FileProcessor());
        fileWorker.execute(new WatcherProcessor());
        this.blockPool = blockPool;
    }

    @Override
    public void addToWaitingPool(String srName, FSPacket packet, HandleResultCallback callback) {
        try {
            fileWaiting.put(new WriteFileRequest(srName, packet, callback));
        } catch (InterruptedException e) {
            LOG.error("enqueue the write request is interrupted!");
        }
        LOG.info("waiting pool size is [{}]", fileWaiting.size());
        LOG.info("storage [{}] : file[{}] waiting for write.", srName, packet.getFileName());
    }

    @Override
    public Block appendToBlock(String srName, FSPacket packet, HandleResultCallback callback) {
        String fileName = packet.getFileName();
        String writeID = packet.getWriteID();
        // file waiting for write
        if (packet.isTheFirstPacketInFile()) {
            LOG.info("After processor : storage [{}] : file[{}] waiting for write.", srName, fileName);
        }
        LOG.debug("writing the file [{}]", packet.getWriteID());
        try {
            BlockValue blockValue = blockcache.get(new BlockKey(srName, packet.getWriteID()));
            if (blockValue == null) {
                HandleResult result = new HandleResult();
                result.setSuccess(false);
                result.setCause(new Exception("error when append block into blockCache,cause by long time no write"));
                LOG.error("can not get block value of file[{}] in blockcache", packet.getWriteID());
                callback.completed(result);
                return null;
            }
            if (packet.getData().length + blockValue.getBlockPos() > blockSize) {
                HandleResult result = new HandleResult();
                result.setSuccess(false);
                result.setCause(new Exception("error when append block into block,cause error packet size"));
                LOG.error("error when append block into block,cause error packet size[{}]", packet.getData().length);
                callback.completed(result);
                return null;
            }
            boolean needflush = blockValue.appendPacket(packet.getData());
            //flush a file
            if (packet.isLastPacketInFile()) {
                if (packet.getBlockOffsetInFile(blockSize) == 0) {
                    writer.write(srName, blockValue.getRealData(),
                                 new WriteFileCallback(callback, fileName, false));
                    LOG.info("flushing a small file[{}] into the data pool", fileName);
                    blockValue.releaseData();
                    blockcache.remove(new BlockKey(srName, writeID));
                } else {
                    // we should flush the last block to get its fid
                    writer.write(srName, blockValue.getRealData(),
                                 new WriteBlockCallback(srName, callback, packet, true));
                    LOG.info("flushing the last block of file [{}] into the data pool ", fileName);
                    blockValue.releaseData();
                }
                return null;
            }
            if (needflush) { //flush a block
                writer.write(srName, blockValue.getBytes(),
                             new WriteBlockCallback(srName, callback, packet, packet.isLastPacketInFile()));
                LOG.info("flush a block of file[{}] into data pool ", fileName);
                blockValue.reset();
                return null;
            }
            if (packet.isTheFirstPacketInFile()) {
                LOG.info("response for the next packet of this file :seqno [{}]", packet.getSeqno());
            }
            if (packet.isLastPacketInFile()) {
                LOG.info("the last packet of file [{}] has arrived", fileName);
            }
            HandleResult handleResult = new HandleResult();
            LOG.debug("packet[{}] append to block and still not flushed。", packet);
            handleResult.setNextSeqno(packet.getSeqno());
            handleResult.setToContinue();
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

    static class BlockKey {
        private String srName;
        private String writeID;

        public BlockKey(String srName, String writeID) {
            this.srName = srName;
            this.writeID = writeID;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            BlockKey blockKey = (BlockKey) o;
            return Objects.equals(srName, blockKey.srName)
                && writeID.equals(blockKey.writeID);
        }

        @Override
        public int hashCode() {
            return Objects.hash(srName, writeID);
        }
    }

    class BlockValue {
        private Block data;
        private List<String> fids = new ArrayList<>();
        private String storage;
        private String file;
        private String writeID;
        private volatile long accessTime;
        private volatile int clearTimeOut = Configs.getConfiguration().getConfig(RegionNodeConfigs.CLEAR_TIME_THRESHOLD);
        ClearTimerTask fooTimerTask = new ClearTimerTask(clearTimeOut);

        public BlockValue(Block block, String storage, String file, String writeID) {
            this.data = block;
            accessTime = System.currentTimeMillis();
            this.storage = storage;
            this.file = file;
            this.writeID = writeID;
            Timer timer = new Timer();
            timer.schedule(fooTimerTask, clearTimeOut, clearTimeOut);
        }

        public byte[] getRealData() {
            return data.getRealData();
        }

        public int getBlockPos() {
            return data.getDataOffsetInBlock();
        }

        public boolean appendPacket(byte[] pdata) {
            data.appendPacket(pdata);
            accessTime = System.currentTimeMillis();
            return data.isBlockSpill();
        }

        boolean isPutBack() {
            return data == null;
        }

        public byte[] writeFile(String fileName) {
            StringBuilder sb = new StringBuilder();
            for (String fid : fids) {
                sb.append(fid).append("\n");
            }
            accessTime = System.currentTimeMillis();
            LOG.info("file[{}] : fids :\n[{}]", fileName, sb);
            return sb.toString().getBytes();
        }

        public void reset() {
            data.reset();
        }

        public void releaseData() {
            LOG.debug("prepare decrement fileWritingCount , now its size is [{}]", fileWritingCount.get());
            try {
                LOCK.lock();
                LOG.debug("decrementing: get the lock");
                data.reset();
                blockPool.putbackBlocks(data);
                fileWritingCount.decrementAndGet();
                LOG.info("decrement fileWritingCount , now its size is [{}]", fileWritingCount.get());
                wasSignalled = true;
                allowWrite.signal();
                data = null;
            } finally {
                LOCK.unlock();
            }
        }

        public void addFid(String fid) {
            fids.add(fid);
            accessTime = System.currentTimeMillis();
        }

        public byte[] getBytes() {
            return data.getBytes();
        }

        class ClearTimerTask extends TimerTask {
            long timeout;

            public ClearTimerTask(long timeout) {
                this.timeout = timeout;
            }

            @Override
            public void run() {
                if (System.currentTimeMillis() - accessTime > timeout) {
                    LOG.info("clear a file [{}] out of blockcache.", file == null ? writeID : file);
                    // 3. clear file on heap
                    if (!isPutBack()) {
                        releaseData();
                    }
                    BlockValue remove = blockcache.remove(new BlockKey(storage, writeID));
                    if (remove == null) {
                        cancel();
                    }

                }
            }
        }
    }

    private class WriteBlockCallback implements StorageRegionWriteCallback {
        private HandleResultCallback callback;
        private String storageName;
        private String fileName;
        private Boolean isFileFinished;
        private long blockOffsetInfile;
        private long seqno;
        private String writeID;

        /**
         * 用来处理写大文件一个block的结果，如果一个block作为file写入的，那么它应该在writeFileCallback中处理
         * 1. 不是最后一个block
         * 2. 是最后一个block
         * 上面两种情况都触发一个对blockcache的写fid操作
         * 并将阶段性结果返回给客户端
         */
        public WriteBlockCallback(String srName, HandleResultCallback callback, FSPacket packet, boolean isFileFinished) {
            this.callback = callback;
            this.storageName = srName;
            this.fileName = packet.getFileName();
            this.isFileFinished = isFileFinished;
            this.blockOffsetInfile = packet.getBlockOffsetInFile(blockSize);
            this.seqno = packet.getSeqno();
            this.writeID = packet.getWriteID();
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
            LOG.info("should put back the block of file [{}]", fileName);
            HandleResult result = new HandleResult();
            // 写回的fid不正确的时候，删除这个正在写的文件
            if ("".equals(fid) || fid == null) {
                BlockValue blockValue = blockcache.remove(new BlockKey(storageName, writeID));
                if (blockValue != null && fileWritingCount.get() > 0) {
                    LOG.info("release data of file [{}]", fileName);
                    blockValue.releaseData();
                }
                result.setSuccess(false);
                LOG.error("dataengine return a null fid of " + isFileFinished + " file :[{}] on flush a block stage", fileName);
                result.setCause(new BRFSException("flush file or block error"));
                callback.completed(result);
                return;
            }
            // 1. blockcache中填写fid
            try {
                BlockValue blockValue = blockcache.get(new BlockKey(storageName, writeID));
                if (blockValue == null) {
                    result.setSuccess(false);
                    LOG.error("flush file or block error,cause there is no [{}] in blockcache", fileName);
                    result.setCause(new BRFSException("flush file or block error,cause there is no element in blockcache"));
                    callback.completed(result);
                    return;
                }
                blockValue.addFid(fid);
                if (!isFileFinished) {
                    String response = "seqno:" + seqno
                        + " filename:" + fileName
                        + " storageName:" + storageName
                        + " done flush";
                    // DONE flush a block
                    result.setToContinue();
                    result.setNextSeqno(seqno);
                    callback.completed(result);
                    LOG.info(response);
                } else {
                    byte[] data = blockValue.writeFile(fileName);
                    writer.write(storageName, data,
                                 new WriteFileCallback(callback, fileName, true));
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
        public void error(Throwable cause) {
            LOG.error("file[{}]seqno:" + seqno
                          + " blockOffsetInfile：" + blockOffsetInfile
                          + " flush error！", fileName);
            BlockValue blockValue = blockcache.remove(new BlockKey(storageName, writeID));
            if (blockValue != null && fileWritingCount.get() > 0) {
                blockValue.releaseData();
            }
            callback.completed(new HandleResult(false));
        }
    }

    private static class WriteFileCallback implements StorageRegionWriteCallback {

        private HandleResultCallback callback;
        private String fileName;
        private boolean isBigFile;

        public WriteFileCallback(HandleResultCallback callback, String file, boolean b) {
            this.callback = callback;
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
            LOG.info("prepare response a fid [{}]!", fid);
            HandleResult result = new HandleResult();
            if ("".equals(fid) || fid == null) {
                result.setSuccess(false);
                LOG.error("dataengine return a null fid of  :[{}] on flush a file stage", fileName);
                result.setCause(new BRFSException("flush file or block error"));
                callback.completed(result);
                return;
            }
            try {
                Preconditions.checkNotNull(fid);
                if (isBigFile) {
                    fid = FidBuilder.setFileType(fid);
                }
                LOG.debug("flushed a file,fid[{}]", fid);
                Preconditions.checkNotNull(fid);
                result.setData(fid.getBytes());
                result.setSuccess(true);
            } catch (BRFSException e) {
                LOG.error("can not set big file flag decode fids", e);
                result.setSuccess(false);
                result.setCause(e);
            }

            callback.completed(result);
        }

        @Override
        public void error(Throwable cause) {
            callback.completed(new HandleResult(false));
        }
    }

    private class FileProcessor implements Runnable {
        @Override
        public void run() {
            if (!runningState.compareAndSet(false, true)) {
                LOG.error("can not execute write file worker again, because it's started!",
                          new IllegalStateException("Write file worker has been started!"));
                return;
            }
            LOG.debug("start process waiting request!");
            WriteRequest unhandledRequest;

            while (true) {
                LOG.debug("loop for writing: start");
                if (quit && fileWaiting.isEmpty()) {
                    LOG.info("stop poll request for writing,[{}]", quit);
                    break;
                }
                try {
                    unhandledRequest = fileWaiting.take();
                    LOG.debug("loop for writing: start take a element");
                    LOCK.lock();
                    LOG.debug("loop for writing: get a element");
                    while (fileWritingCount.get() >= blockPoolSize) {
                        LOG.info("loop for writing : wait until allow to write");
                        if (!wasSignalled) {
                            allowWrite.await();
                        }
                    }
                    LOG.info("Processor : the waiting request size is [{}]", fileWaiting.size());
                    if (unhandledRequest.ifRequestIsTimeOut()) {
                        LOG.info("abandon a file write request because of Time out");
                        HandleResult result = new HandleResult();
                        result.setSuccess(false);
                        result.setCause(new Exception("abandon a file write request because of Time out"));
                        unhandledRequest.getHandleResultCallback().completed(result);
                        continue;
                    }
                    fileWritingCount.incrementAndGet();
                    BlockKey blockKey = new BlockKey(unhandledRequest.getSrName(),
                                                     unhandledRequest.getFsPacket().getWriteID());
                    Block block = blockPool.getBlock();
                    blockcache.put(blockKey, new BlockValue(block, unhandledRequest.getSrName(),
                                                            unhandledRequest.getFsPacket().getFileName(),
                                                            unhandledRequest.getFsPacket().getWriteID()));
                    LOG.info("Processor : writing file [{}]", unhandledRequest.getFsPacket().getFileName());
                    appendToBlock(unhandledRequest.getSrName(),
                                  unhandledRequest.getFsPacket(),
                                  unhandledRequest.getHandleResultCallback());
                } catch (InterruptedException e) {
                    LOG.error("data consumer interrupted.");
                } catch (Exception e) {
                    LOG.error("process data error", e);
                } finally {
                    wasSignalled = false;
                    LOCK.unlock();
                }

            }

            LOG.info("Write file worker is shut down!");
        }

    }

    private class WatcherProcessor implements Runnable {
        @Override
        public void run() {

            while (!quit || !fileWaiting.isEmpty()) {
                LOG.info("Watcher : enqueue file count is [{}]"
                             + "file writing count is [{}]"
                             + "file in blockcache is [{}]",
                         fileWaiting.size(),
                         fileWritingCount.get(),
                         blockcache.size());
                try {
                    Thread.sleep(300000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }

            LOG.info("Write file worker is shut down!");
        }

    }
}
