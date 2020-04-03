package com.bonree.brfs.duplication.datastream.blockcache;

import com.bonree.brfs.common.net.http.HandleResult;
import com.bonree.brfs.common.net.http.HandleResultCallback;
import com.bonree.brfs.common.net.http.data.FSPacket;
import com.bonree.brfs.common.utils.JsonUtils;
import com.bonree.brfs.duplication.datastream.writer.DefaultStorageRegionWriter;
import com.bonree.brfs.duplication.datastream.writer.StorageRegionWriteCallback;
import com.bonree.brfs.duplication.datastream.writer.StorageRegionWriter;
import com.google.common.base.Preconditions;
import com.google.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * 用来记录正在写入的文件
 * sn [storageName => files]
 * files [file => block_fids]
 * block_fids [offset => block_fid]
 * block_fid [fid => block]
 * @author wangchao
 * @date 2020/3/23 - 2:40 下午
 */
public class BlockManager implements BlockManagerInterface{
    private static final Logger LOG = LoggerFactory.getLogger(BlockManager.class);
    //storage => fileEntry
    private BlockCache blockCache = new BlockCache();
    //    private static FileOnConstruction INSTANCE ;
    private BlockPool blockPool;

    private DefaultStorageRegionWriter writer;

//    private RocksDBManager rocksDBManager ;

    private volatile AtomicBoolean isAquireNewBlock = new AtomicBoolean();

    ReentrantLock lock = new ReentrantLock();
    Condition noAquireNewBlock = lock.newCondition();
//    @Inject
//    public BlockManager(BlockPool blockPool, StorageRegionWriter writer , RocksDBManager rocksDBManager) {
//        this.blockPool = blockPool;
//        this.writer = (DefaultStorageRegionWriter) writer;
//        this.rocksDBManager = rocksDBManager;
//    }

    @Inject
    public BlockManager(BlockPool blockPool, StorageRegionWriter writer) {
        this.blockPool = blockPool;
        this.writer = (DefaultStorageRegionWriter) writer;
    }

    /**
     * 添加一个packet到blockcache中的对应block上
     * 1. 更新期望值，以便block或者文件在收到全部成员后触发落盘操作
     * 2. 追加packet到block，更新实际值
     * 1.2 getBlock
     * 2. 添加到block上
     * 2.1 写完数据以后检查当前block是否写满了或者是收到文件结束标识（先处理block，再处理文件）
     * ，满足条件调用writer，并标记当前block条目为完成
     * 3. 检查文件状态
     * 此时要复制packet的真实数据到block中
     * 更新对应文件的写入时间
     *
     * @param packet
     * @return block
     */
    public synchronized Block appendToBlock(FSPacket packet, HandleResultCallback callback) {
        Block block = getBlock(packet);

        if(block == null){
            //等待blockpool 失败
            return null;
        }
        FileEntry file = getFile(packet);
        //更新文件时间戳
        file.updateTimestamp();
        int storage = packet.getStorageName();
        long packetOffsetInFile = packet.getOffsetInFile();
        long blockOffsetInFile = packet.getBlockOffsetInFile(block.getSize());
        long blockSizeInFile;
        long packetSizeInblock;
        String fileName = packet.getFileName();
        // here we get a block
        // 如果是最后一个packet，更新block计数器和file计数器
        if (packet.isLastPacketInFile()) {
            // 1. 计算文件中的block数
            blockSizeInFile = packetOffsetInFile / block.getSize() + 1;
            blockCache.getStorage(storage).getFile(fileName).setExpectBlockSize(blockSizeInFile);
            // for 不满1024个packet的block
            // 2. 计算当前block中的packet数，更新block的为最后一个block，但不一定是最后一个完整收到的block
            packetSizeInblock = packetOffsetInFile % block.getSize() / block.expectPacketLen + 1;
            block.setExpectPacketSize(packetSizeInblock);
            block.setTheLastInFile();
        }
        // 填充到block对应位置上
        block.appendPacket(packet.getData(), packet.getPacPos(blockPool.getBlockSize()), packet.isLastPacketInFile());
        // 写入的真实数据
        byte[] realData ;
        // 检查block状态,每个packet都要检查，因为可能比最后一个packet到的晚
        // 检出大于packet但是小于等于block的小文件
        // 1. 当前packet是否是最后一个到达的packet,即一个block是否收完了
//        LOG.debug("收到的seq:[{}],block[{}]:已收到[{}]个packet，期望收到{}个", packet.getSeqno(),block.getId(),block.,block.getExpectPacketsize());
        if (block.isAcceptedAllPackets()) {
            realData = block.getRealData();
            file.incBlockCount();
            // 1.1. 如果当前block是文件中的唯一一个块,则它是只有一个块的小文件
            if(block.isTheLastInFile() && getFileBlockSize(storage, fileName)==1){
                // 释放block
                blockCache.releaseBlock(storage,fileName,blockOffsetInFile);
                clearFile(storage,fileName);

                writer.write(storage,realData,
                        new WriteFileCallback(callback,storage,fileName));
                LOG.debug("在[{}]中写了一个小于等于64M但大于等于64k的小文件[{}]", storage, fileName);

            }else {
                blockCache.releaseBlock(storage, fileName, blockOffsetInFile);
                // 1.2 大文件的一个block
                writer.write(storage, realData,
                        new WriteBlockCallback(callback, packet, block,getFile(packet).isAcceptedAllBlocks()));
                LOG.debug("在追加packet到block时，storage[{}]-file[{}]中的一个block[{}] 写满了。落盘",storage,fileName,blockOffsetInFile);
            }
            return block;
        }
        HandleResult handleResult = new HandleResult();

        if(block == null){
            LOG.debug("packet[{}]没有申请到block",packet.getSeqno());
            handleResult.setSuccess(false);
            handleResult.setCause(new Exception("packet" + packet.getSeqno() + "没有申请到block"));
            callback.completed(handleResult);
            return block;
        }
        //如果是一个大文件的一个block，但是并没有写满,只做追加动作，前面完成了
        LOG.debug("packet[{}] 追加到block[{}],且未写满block，累积在内存中。。。，",packet,block);
        handleResult.setNextSeqno(packet.getSeqno());
        handleResult.setCONTINUE();
        callback.completed(handleResult);
        return block;
    }
    private FileEntry getFile(int storage,String file){
        if(!isFileExist(storage,file)){
            return null;
        }
        return blockCache.getFile(storage,file);
    }
    private FileEntry getFile(FSPacket packet) {
        return getFile(packet.getStorageName(),packet.getFileName());
    }

    /**
     * 新的packet被append，是否需要新申请一个block
     * 新申请的条件为files中没有记录
     *
     * @param packet
     * @return
     */
    public synchronized boolean shouldAquireNewBlock(FSPacket packet) {
        //1. 检查之前存在该block
        if (!isBlockExsit(packet)) {
            //不存在该block 需要申请一个
            return true;
        }
        return false;
    }
    /**申请blockcache的读*/
    private void aquireRead() {
        blockCache.readLock();
    }

    /**
     * 检查是否存在对应的block
     *
     * @param packet
     * @return
     */
    public boolean isBlockExsit(FSPacket packet) {
        boolean flag = blockCache.getBlock(packet.getStorageName(), packet.getFileName(), packet.getBlockOffsetInFile(blockPool.getBlockSize())) != null;
        return flag;
    }

    private void releaseRead() {
        blockCache.endRead();
    }

    /**
     * 一个packet对应的文件是否之前就存在
     *
     * @param packet
     * @return
     */
    public boolean isFileExist(FSPacket packet) {

        return isFileExist(packet.getStorageName(), packet.getFileName());
    }

    private boolean containBlock(int storage, String file, long offset) {
        return blockCache.containBlock(storage, file, offset);
    }


    /**
     * 返回一个文件拥有的block数
     *
     * @return file's block size if it is exist, -1 if it is not exist
     */
    public int getFileBlockSize(int storage, String fileName) {
        if (blockCache.getStorage(storage) == null) {
            return 0;
        }
        if (blockCache.getStorage(storage).getFile(fileName) == null) {
            return 0;
        }
        return blockCache.getStorage(storage).getFile(fileName).size();
    }

    /**
     * 整理将一个文件所有的blockid写入一个dn的数据
     *
     * @param storageName
     * @param fileName
     * @return 返回需要落盘的索引文件内容
     */
    public byte[] writeFile(int storageName, String fileName) {
        StringBuilder sb = new StringBuilder("::brfs-index-file::storage[" + storageName + "]file[" + fileName + "]\n");
        FileEntry file = blockCache.getStorage(storageName).getFile(fileName);
        Preconditions.checkNotNull(file, "file[{}] should't be null", fileName);
        Map<Long, BlockOrFidEntry> offsetToBlock = file.getOffsetToBlock();
        for (Long offset : offsetToBlock.keySet()) {
            sb.append(offset).append(" ").append(offsetToBlock.get(offset)).append("\n");
        }
        return sb.toString().getBytes();
    }


    public String getFid(int storage, String fileName, long offset) {
        return blockCache.getStorage(storage).getFile(fileName).getBlockEntry(offset).getFid();
    }


    /**
     *     处理callback时 用
     * @deprecated
     */
    public boolean isSmallFile(int storageName, String fileName) {
        if (!isFileExist(storageName, fileName)) {
            //缓存中没有落盘的文件时，是一个小文件
            return true;
        }
        //缓存中该文件只有一个block，则这是个小文件
        return getFileBlockSize(storageName, fileName) == 1;
    }

    private boolean isFileExist(int storageName, String fileName) {
        StorageEntry storage = blockCache.getStorage(storageName);
        if (null == storage) {//如果sr不存在
            return false;
        } else if (null == storage.getFile(fileName)) {//如果文件不存在
            return false;
        }
        return true;
    }

    // 只有blockmanager可以写fid
    // 在这里触发大文件的索引写入
    public synchronized boolean setFidAndReleaseBlock(int storageName, String fileName, long blockOffsetInfile, String fid,HandleResultCallback callback) {
        if (isFileExist(storageName, fileName)) {
            FileEntry file = getFile(storageName, fileName);
            file.updateTimestamp();
            BlockOrFidEntry blockOrFidEntry = blockCache.getStorage(storageName).getFile(fileName).getOffsetToBlock().get(blockOffsetInfile);
            Preconditions.checkNotNull(blockOrFidEntry, "block在向datapool中写数据之前应该在blockcache中注册一个blockOrFidEntry");
            blockOrFidEntry.setFid(fid);
            //如果最后一个block的的fid也写入了
            if(file.isAcceptedAllBlocks()){
                byte[] data = writeFile(storageName, fileName);
                clearFile(storageName,fileName);
                writer.write(storageName,data,
                        new WriteFileCallback(callback,storageName,fileName));
                LOG.info("在[{}]中写了一个大文件索引文件[{}]", storageName, fileName);
            }
            return true;
        }
        LOG.warn("向block缓存中写fid时对应文件是空的，这很不正常！！");
        return false;
    }

    public BlockPool getBlockPool() {
        return blockPool;
    }

    public void setBlockPool(BlockPool blockPool) {
        this.blockPool = blockPool;
    }

    public long getBlockSize() {
        return blockPool.getBlockSize();
    }

    class BlockCache {
        ReadWriteLock lock = new ReentrantReadWriteLock();
        Map<Integer, StorageEntry> storages = new ConcurrentHashMap<>();

        public BlockCache() {

        }

        public Block  getBlock(int storage, String fileName, long offset) {
            BlockOrFidEntry blockEntry = getBlockOrFidEntry(storage,fileName,offset);
            if (blockEntry == null) {
                return null;
            }
            return blockEntry.getBlock();
        }

        public BlockOrFidEntry getBlockOrFidEntry(int storage, String fileName, long offset){
            if (!isFileExist(storage, fileName)) return null;
            BlockOrFidEntry blockEntry = storages.get(storage).getFile(fileName).getBlockEntry(offset);
            return blockEntry;
        }

        public StorageEntry getStorage(int storageName) {
            return storages.get(storageName);
        }

        //blockCache中是已经存在某个命名空间
        public boolean isStorageExist(int storageName) {
            return storages.containsKey(storageName);
        }

        /**
         * 在blockCatche中注册一个命名空间
         *
         * @param storageName
         */
        public void addStorage(int storageName, StorageEntry storage) {
            storages.put(storageName, storage);
        }

        public boolean containBlock(int storage, String file, long blockOffsetInfile) {

            if (!containStorage(storage)) {
                return false;
            }
            StorageEntry curStorage = blockCache.getStorage(storage);
            if (!curStorage.containFile(file)) {
                return false;
            }
            FileEntry curfile = curStorage.getFile(file);
            if (!curfile.containOffset(blockOffsetInfile)) {
                return false;
            }
            BlockOrFidEntry blockEntry = curfile.getBlockEntry(blockOffsetInfile);
            return blockEntry.getBlock() != null;
        }

        /**
         * 注册一个block到blockcache上
         * 这里可能存在线程安全问题
         *
         * @param storageName
         * @param fileName
         * @param blockOffsetInFile
         * @param curBlock
         */
        public void addBlock(int storageName, String fileName, long blockOffsetInFile, Block curBlock) {
            if (!storages.containsKey(storageName)) {
                storages.put(storageName, new StorageEntry(storageName));
            }
            StorageEntry storageEntry = storages.get(storageName);
            if (!storageEntry.containFile(fileName)) {
                storageEntry.putFile(fileName, new FileEntry(storageName,fileName));
            }
            FileEntry file = storageEntry.getFile(fileName);
            if (!file.containOffset(blockOffsetInFile)) {
                //在blockcache中注册了block
                file.putOffsetToBlock(blockOffsetInFile, new BlockOrFidEntry(curBlock));
            }
        }

        public int aboundFile(int storageName, String fileName) {
            if (!isFileExist(storageName, fileName)) {
                return 0;
            }
            return storages.get(storageName).aboundFile(fileName);
        }

        public boolean containStorage(int storageName) {
            return storages.containsKey(storageName);
        }
        //释放block，保留offsetblockentry
        public void releaseBlock(int storageName, String fileName, long offsetInFile) {
            BlockOrFidEntry blockOrFidEntry = getBlockOrFidEntry(storageName, fileName, offsetInFile);
            if(blockOrFidEntry == null){
                LOG.warn("尝试释放一个不存在的块，可能块被泄露了");
                return;
            }
            blockOrFidEntry.releaseBlock();
        }

        public FileEntry getFile(int storage, String file) {
            return getStorage(storage).getFile(file);
        }

        public void readLock() {
            lock.readLock().lock();
        }

        public void endRead() {
            lock.readLock().unlock();
        }
    }

    /**
     * sr条目
     */
    class StorageEntry {
        private int StrorageName;
        private Map<String, FileEntry> files = new ConcurrentHashMap<>();

        public StorageEntry(int strorageName) {
            StrorageName = strorageName;
        }

        public int getStrorageName() {
            return StrorageName;
        }

        public Map<String, FileEntry> getFiles() {
            return files;
        }

        public FileEntry getFile(String fileName) {
            return files.get(fileName);
        }

        public boolean containFile(String fileName) {
            return files.containsKey(fileName);
        }

        public void putFile(String fileName, FileEntry file) {
            files.put(fileName, file);
        }

        //清除文件
        public int aboundFile(String fileName) {
            FileEntry remove = files.remove(fileName);
            Map<Long, BlockOrFidEntry> offsetToBlock = remove.getOffsetToBlock();
            //清除的时候要尝试回收该文件下的block
            for (Long l : offsetToBlock.keySet()) {
                BlockOrFidEntry blockOrFidEntry = offsetToBlock.get(l);
                Block block = blockOrFidEntry.getBlock();
                if (null != block) {
                    blockPool.putbackBlocks(block);
                }
            }
            //在一个通道写入失败后
            remove.setExpectBlockSize(0);
            //取消删除任务
            remove.cancelTimer();
            return remove.size();
        }
    }

    /**
     * 文件条目
     */
    class FileEntry {

        private boolean turnInfoBigFile = false;
        private int storage;
        private String fileName;
        private Map<Long, BlockOrFidEntry> offsetToBlock = new ConcurrentHashMap<>();
        private AtomicLong packetLen = new AtomicLong(0l);
        private volatile long timestamp = System.currentTimeMillis();
        ClearTimerTask fooTimerTask = new ClearTimerTask(3000); // 2. 创建任务对象

        private Timer timer = new Timer(); // 1. 创建Timer实例，关联线程不能是daemon(守护/后台)线程

        /**
         * 文件应该包含的block数
         */
        private AtomicLong expectBlockSize = new AtomicLong(0);
        private AtomicLong actualBlockSize = new AtomicLong(0);

        public long getExpectBlockSize() {
            return expectBlockSize.get();
        }

        public void setExpectBlockSize(long expectBlockSize) {
            this.expectBlockSize.set(expectBlockSize);
        }
        /**block计数器+1*/
        public void incBlockCount() {
            actualBlockSize.incrementAndGet();
        }
        /**获得实际收完的block数*/
        public long getActualBlockSize(){
            return actualBlockSize.get();
        }
        public FileEntry(int storage , String fileName) {
            this.fileName = fileName;
            timer.schedule(fooTimerTask, 3000L, 3000L); // 3. 通过Timer定时定频率调用fooTimerTask的业务代码
            this.storage = storage;
        }

        public void putOffsetToBlock(long offset, BlockOrFidEntry block) {
            offsetToBlock.put(offset, block);
            //如果添加一个offset=》block到此文件时 offset大于1，则将此文件视作大文件
            if (offsetToBlock.size() > 1) {
                turnInfoBigFile = true;
            }
        }
        public List<String> getFids(){
            String fid ;
            ArrayList<String> fids = new ArrayList<>();
            for(long offset:offsetToBlock.keySet()){
                fid = offsetToBlock.get(offset).getFid();
                if(null == fid && fid!=""){
                    continue;
                }
                fids.add(fid);
            }
            return fids;
        }
        public String getFileName() {
            return fileName;
        }

        public Map<Long, BlockOrFidEntry> getOffsetToBlock() {
            return offsetToBlock;
        }

        public BlockOrFidEntry getBlockEntry(long offset) {
            return offsetToBlock.get(offset);
        }

        public boolean containOffset(long blockOffsetInFile) {
            return offsetToBlock.containsKey(blockOffsetInFile);
        }

        public int size() {
            return offsetToBlock.size();
        }
        /**是否收到了所有的block*/
        public boolean isAcceptedAllBlocks() {
            return expectBlockSize.get()!=0?expectBlockSize.get()==actualBlockSize.get():false;
        }

        public void updateTimestamp() {
            timestamp = System.currentTimeMillis();
        }

        public void cancelTimer() {
            timer.cancel();
        }

        class ClearTimerTask extends TimerTask {
            long timeout;
            public ClearTimerTask(long timeout) {
                this.timeout = timeout;
            }
            @Override
            public void run() {
                if(System.currentTimeMillis()-timestamp > timeout){
                    //1. get all fid of this file
                    List<String> fids = getFids();
                    for (String fid : fids) {
                        //2. remove by fid
                        LOG.debug("发布一个删除任务fid:[{}]", fid);
                    }
                    LOG.debug("因超时丢弃一个文件:[{}]，已写入的block", fileName);
                    // 3. clear file on heap
                    int i = blockCache.aboundFile(storage, fileName);
                    if(i ==0){
                        //如果已经删除，取消任务
                        cancel();
                    }
                }
            }
        }
    }

    /**
     * fid和block的对应关系
     * block还没写入的时候，fid是空，isStored=false
     * block写入的时候，block为空，isStored=true
     */
    class BlockOrFidEntry {
        /**
         * 该blockOrfid对应的block是否已经执行落盘了
         */
        private boolean isStored = false;
        private String fid;
        private Block block;
        //private long lastPacketSeqno;

        public BlockOrFidEntry(Block block) {
            this.block = block;
        }

        public Block getBlock() {
            if (isStored) return null;
            return block;
        }

        public void doneStored(String fid) {
            block = null;
            this.fid = fid;
            isStored = true;
        }

        public void setFid(String fid) {
            this.fid = fid;
            setStored(true);
        }

        public void setStored(boolean stored) {
            isStored = stored;
        }

        public boolean isStored() {
            return isStored;
        }

        public String getFid() {
            return fid;
        }

        public void releaseBlock(){
            if(block==null){
                LOG.debug("尝试释放一个不存在的block！但是存在注册条目");
                return;
            }
            blockPool.putbackBlocks(block);
            block = null;
        }
    }

    //从OnConstruction中去掉file，放弃对file的写
    public void clearFile(int storageName, String fileName) {
        int size = blockCache.aboundFile(storageName, fileName);
        LOG.info("写完了[{}:{}]的[{}]个block", storageName, fileName, size);
    }

    /**
     * 从blockManager中取一个block来存packet，没有的时候会创建一个
     *
     * @param packet
     * @return block
     */
    private synchronized Block getBlock(FSPacket packet){
        long blockOffsetInFile = packet.getBlockOffsetInFile(blockPool.getBlockSize());
        Block curBlock = null;
        //同一时间只允许一个申请block请求,这样可以避免所有同一个block的packet请求都去申请新块
        //极端情况下，完全没有block可用时还是会所有都申请,只是顺序申请
//        while(shouldAquireNewBlock(packet)){
//            try {
//                Thread.sleep(30);
//            } catch (InterruptedException e) {
//                e.printStackTrace();
//            }
//            noAquireNewBlock.await();
//            if(isAquireNewBlock.compareAndSet(false,true)){
//                //等待同一个block中的其他packet请求到块或者自己被获许可以申请一个块
//                break;
//            }
//            LOG.debug("[{}]正在等待申请block",packet.getSeqno());
//
//        }


        //同一时间只允许一个申请block请求,这样可以避免所有同一个block的packet请求都去申请新块
        //极端情况下，完全没有block可用时还是会所有都申请,只是顺序申请
        // 需要申请新块，但是已经有在做创建新块的请求
        if(shouldAquireNewBlock(packet) && isAquireNewBlock.get() ){

            lock.lock();
            try {
                // 二次检查
                while (shouldAquireNewBlock(packet)){
                    noAquireNewBlock.await();
                }
                isAquireNewBlock.compareAndSet(false,true);
            } catch (InterruptedException e) {
                LOG.warn("等待申请一个新块的时候中断");
                e.printStackTrace();
            }finally {
                lock.unlock();
            }
        }
        //1. 自己获得许可
        if (shouldAquireNewBlock(packet)) {
            lock.lock();
            try {
                curBlock = blockPool.getBlock();
            } catch (InterruptedException e) {
                LOG.warn("文件[{}]的写入被中断");
                e.printStackTrace();
            }
            if(curBlock == null) {
                //这里应该想办法去拒绝该文件之后来的包
                noAquireNewBlock.signal();
                isAquireNewBlock.compareAndSet(true,false);
                LOG.warn("block pool 已经到最大值，等待写入超时，驳回写入请求！");
                return null;
            }
            curBlock.setOffsetInFile(blockOffsetInFile);
            blockCache.addBlock(packet.getStorageName(), packet.getFileName(), blockOffsetInFile, curBlock);
            noAquireNewBlock.signal();
            isAquireNewBlock.compareAndSet(true,false);
            lock.unlock();
            LOG.debug("append packet[{}]时新申请了一个block[{}]并注册到blockcache中", packet, curBlock);
        } else {//好兄弟获得许可
            curBlock = blockCache.getBlock(packet.getStorageName(), packet.getFileName(), packet.getBlockOffsetInFile(blockPool.getBlockSize()));
            if(curBlock == null) {
                LOG.warn("blockcache中没有对应block，它应该有存在！");
                return null;
            }
            LOG.debug("从blockcache中取到一个block", curBlock);
        }
        return curBlock;
    }

    /**
     * write block只有在情况会触发
     * 大文件的一个块落盘
     * 处理方式，
     * 1.将fid写回到blockcache中
     */
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
         * @param block
         */
        public WriteBlockCallback(HandleResultCallback callback, FSPacket packet, Block block,boolean isFileFinished) {
            this.callback = callback;
            this.storageName = packet.getStorageName();
            this.fileName = packet.getFileName();
            this.isFileFinished = isFileFinished;
            this.blockOffsetInfile = packet.getBlockOffsetInFile(blockPool.getBlockSize());
            this.seqno = packet.getSeqno();
            this.block = block;
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
            setFidAndReleaseBlock(storageName, fileName, blockOffsetInfile, fid,callback);
            LOG.debug("获得大文件的一个block的fid[{}]", fid);
            if(!isFileFinished){
                String response = "seqno:" + seqno +
                        " filename:" + fileName +
                        " storageName:" + storageName +
                        " done flush";
                LOG.debug(response);
                result.setCONTINUE();
                result.setNextSeqno(seqno);
                callback.completed(result);
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



    /**
     * write block只有在情况会触发
     * 大文件的一个块落盘
     * 处理方式，
     * 1.将fid写回到blockcache中
     * 2.清理block
     */
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
            blockCache.aboundFile(StorageName,fileName);
        }

        @Override
        public void error() {
            callback.completed(new HandleResult(false));
        }
    }


}

