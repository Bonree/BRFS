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
 * @author wangchao
 * @date 2020/3/20 - 10:49 上午
 */
public class BlockPool {
    private static final Logger LOG = LoggerFactory.getLogger(BlockPool.class);
    private static long waitForBlock = Configs.getConfiguration().GetConfig(RegionNodeConfigs.WAIT_FOR_BLOCK_TIME);

//    ReentrantLock lock = new ReentrantLock();
    // for acquire
//    Condition havaBlock = lock.newCondition();

    private final long blockSize;
    private static final long defaulBlockSize = 64 * 1024 * 1024;
    private int maxCount;
    private final BlockingQueue<Block> reclaimedBlocks;
//    private final float poolSizePercentage;
    //give next new block an id
    private AtomicInteger BlockID = new AtomicInteger(1);
    // mapping from Block IDs to Blocks
    private Map<Integer, Block> blockIdMap = new ConcurrentHashMap<Integer, Block>();


    //for statistics
    private final AtomicLong BlockCount = new AtomicLong();
    public BlockPool(long blockSize, int maxCount, int initialCount) {
        this.blockSize = blockSize;
        this.maxCount = maxCount;
        this.reclaimedBlocks = new LinkedBlockingQueue<>();
        //初始池中管理多少block
        for (int i = 0; i < initialCount; i++) {
            //create block
            Block block = createBlock();
            // lazy init
            block.init();
            //添加到reclaimedBlocks中
            reclaimedBlocks.add(block);
        }
        LOG.debug("block池中的blocksize为[{}]",blockSize);

    }
    /**
     * 从pool中拿一个可用block来用，里面有内容的时候，擦除内容。没有可用的block时，尝试创建一个新的block
     * 如果已经到了blockpool的最大块数，尝试从回收队列中取一个已有的，如果3秒后还是获取不到，则返回空block
     */

    public Block getBlock() throws InterruptedException {
        Block block = reclaimedBlocks.poll();
        if (block != null) {
            block.reset();
//            for statistics
//            reusedBlockCount.increment();
        } else {
            // Make a Block if we have not yet created the maxCount Blocks
            while (true) {
                long created = this.BlockCount.get();
                if (created < this.maxCount) {
                    if (this.BlockCount.compareAndSet(created, created + 1)) {
                        block = createBlock();
                        block.init();
                        break;
                    }
                } else {
                    break;
                }
            }
        }
        // blockpool 已经满了，尝试取一个block，等待3秒
        if(block == null){
            LOG.warn("blockpool 已经写满了，等待[{}]秒.",waitForBlock);
            block = reclaimedBlocks.poll(waitForBlock,TimeUnit.MILLISECONDS);
        }
        return block;
    }
    /**
     * Add the Blocks to the pool, when the pool achieves the max size, it will skip the remaining
     * Blocks
     * @param b
     */
    public void putbackBlocks(Block b) {
        int toAdd = this.maxCount - reclaimedBlocks.size();
        if (b.size == blockSize && toAdd > 0) {
            reclaimedBlocks.add(b);
        } else {
            // remove the Block (that is not going to pool)
            // though it is initially from the pool or not
            removeBlock(b.getId());
        }
    }
    //创建一个新的block，分配一个id
    // 此时尚未分配实际存储空间
    private Block createBlock() {
        Block block;
        int id = BlockID.getAndIncrement();
        assert id > 0;

        block = new Block(id,blockSize);
        //防止GC回收
        this.blockIdMap.put(block.getId(), block);
        return block;
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
