package com.bonree.brfs.duplication.datastream.blockcache;

import com.bonree.brfs.configuration.Configs;
import com.bonree.brfs.configuration.units.RegionNodeConfigs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 用来管理block 数组
 * 1. 流量限制
 * 2. require，release
 */
public class SeqBlockPool implements BlockPool {
    private static final Logger LOG = LoggerFactory.getLogger(SeqBlockPool.class);
    private static long waitForBlock = Configs.getConfiguration().GetConfig(RegionNodeConfigs.WAIT_FOR_BLOCK_TIME);
    private final long blockSize;
    private int maxCount;
    private final BlockingQueue<Block> reclaimedBlocks;
    private AtomicInteger blockID = new AtomicInteger(1);
    // mapping from SeqBlock IDs to Blocks
    private Map<Integer, SeqBlock> blockIdMap = new ConcurrentHashMap<Integer, SeqBlock>();


    //for statistics
    private final AtomicLong blockCount = new AtomicLong();
    public SeqBlockPool(long blockSize, int maxCount, int initialCount) {
        this.blockSize = blockSize;
        this.maxCount = maxCount;
        this.reclaimedBlocks = new LinkedBlockingQueue<>();
        //初始池中管理多少block
        for (int i = 0; i < initialCount; i++) {
            //create SeqBlock
            SeqBlock SeqBlock = createBlock();
            // lazy init
            SeqBlock.init();
            //添加到reclaimedBlocks中
            reclaimedBlocks.add(SeqBlock);
        }
    }
    /**
     * 从pool中拿一个可用block来用，里面有内容的时候，擦除内容。没有可用的block时，尝试创建一个新的block
     * 如果已经到了blockpool的最大块数，尝试从回收队列中取一个已有的，如果3秒后还是获取不到，则返回空block
     */

    public Block getBlock() throws InterruptedException {
        Block SeqBlock = reclaimedBlocks.poll();
        if (SeqBlock != null) {
            SeqBlock.reset();
//            for statistics
//            reusedblockCount.increment();
        } else {
            // Make a SeqBlock if we have not yet created the maxCount Blocks
            while (true) {
                long created = this.blockCount.get();
                if (created < this.maxCount) {
                    if (this.blockCount.compareAndSet(created, created + 1)) {
                        SeqBlock = createBlock();
                        SeqBlock.init();
                        break;
                    }
                } else {
                    break;
                }
            }
        }
        // blockpool 已经满了，尝试取一个block，等待3秒
        if(SeqBlock == null){
            LOG.warn("blockpool 已经写满了，等待[{}]秒.",waitForBlock);
            SeqBlock = reclaimedBlocks.poll(waitForBlock,TimeUnit.MILLISECONDS);
        }
        return SeqBlock;
    }
    /**
     * Add the Blocks to the pool, when the pool achieves the max size, it will skip the remaining
     * Blocks
     * @param b
     */
    public void putbackBlocks(Block b) {
        if(b==null){
            LOG.warn("try to put back a null block , may be some block is miss now ");
            return;
        }
        int toAdd = this.maxCount - reclaimedBlocks.size();
        if (b.getSize() == blockSize && toAdd > 0) {
            reclaimedBlocks.add(b);
        } else {
            // remove the SeqBlock (that is not going to pool)
            // though it is initially from the pool or not
            removeBlock(b.getId());
        }
    }
    //创建一个新的block，分配一个id
    // 此时尚未分配实际存储空间
    private SeqBlock createBlock() {
        SeqBlock SeqBlock;
        int id = blockID.getAndIncrement();
        assert id > 0;

        SeqBlock = new SeqBlock(id,blockSize);
        //防止GC回收
        this.blockIdMap.put(SeqBlock.getId(), SeqBlock);
        return SeqBlock;
    }
    private void removeBlock(int id){
        blockIdMap.remove(id);
    }
    //for test
    protected BlockingQueue<Block> getReclaimedBlocks() {
        return reclaimedBlocks;
    }

    public long getBlockSize() {
        return blockSize;
    }
}
