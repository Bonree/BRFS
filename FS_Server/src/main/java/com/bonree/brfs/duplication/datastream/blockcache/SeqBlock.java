package com.bonree.brfs.duplication.datastream.blockcache;

import com.google.common.base.Preconditions;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author wangchao
 * @date 2020/3/20 - 10:54 上午
 */
public class SeqBlock implements Block {

    private static final Logger LOG = LoggerFactory.getLogger(SeqBlock.class);

    /**
     * Actual underlying data
     */
    protected byte[] data;
    /**
     * block state 未初始化
     */
    protected static final int UNINITIALIZED = -1;
    /**
     * block state 为data分配内存的时候oom
     */
    protected static final int OOM = -2;

    public long getSize() {
        return size;
    }

    //64kfor test，后面做成可以从配置读取
    private static final int defaultBlockLen = 64 * 1024 * 1024;

    /**
     * Size of block in bytes
     */
    protected long size = defaultBlockLen;
    // The unique id associated with the block.
    private final int id;
    private boolean isTheLastInFile = false;

    public boolean isTheLastInFile() {
        return isTheLastInFile;
    }

    public void setTheLastInFile() {
        isTheLastInFile = true;
    }

    public long getOffsetInFile() {
        return offsetInFile;
    }

    public void setOffsetInFile(long offsetInFile) {
        this.offsetInFile = offsetInFile;
    }

    private long offsetInFile;

    /**
     * Offset for the next allocation, or the sentinel value -1 which implies that the block is still
     * uninitialized.
     * 已经填充进来的packet的size，-1代表还未初始化
     */
    protected AtomicInteger realDataSize = new AtomicInteger(UNINITIALIZED);

    /**
     * Total number of allocations satisfied from this buffer
     */
    protected AtomicInteger allocCount = new AtomicInteger();

    public int getId() {
        return id;
    }

    public SeqBlock(int id, long blocksize) {
        this.id = id;
        this.size = blocksize;
    }

    /**
     * byte数组长度是固定的(1024位)，重复利用，这里把offset拨回0，老值不管了，用offset记录有效值的长度
     */
    public void reset() {
        if (realDataSize.get() != UNINITIALIZED) {
            realDataSize.set(0);
            allocCount.set(0);
        }
    }

    //申请一个block的data的空间，这里byte数组最大长度为2的32次方-1
    void allocateDataBuffer() {
        if (data == null) {
            data = new byte[(int) this.size];
        }
    }

    /**
     * Actually claim the memory for this block. This should only be called from the thread that
     * constructed the block. It is thread-safe against other threads calling alloc(), who will block
     * until the allocation is complete.
     */
    public void init() {
        //构造之后block应该是未初始化状态
        assert realDataSize.get() == UNINITIALIZED;
        try {
            allocateDataBuffer();
        } catch (OutOfMemoryError e) {
            //设置block状态为 异常：OOM
            boolean failInit = realDataSize.compareAndSet(UNINITIALIZED, OOM);
            assert failInit; // should be true.
            throw e;
        }
        // Mark that it's ready for use
        // realDataSize位于0位置
        boolean initted = realDataSize.compareAndSet(UNINITIALIZED, 0);
        // We should always succeed the above CAS since only one thread
        // calls init()!
        Preconditions.checkState(initted, "Multiple threads tried to init same chunk");
    }

    /**
     * 在copy一个packet到当前block之前要先检查block是否还有足够长度来存放packet数组
     * 如果在init之前调用alloc(), 调用线程会忙等待到realDataSize被设置.
     *
     * @return 成功alloc的offset, 数据将插入这个位置, or -1 表示没有足够空间
     */
    public int alloc(int size) {
        while (true) {
            int oldOffset = realDataSize.get();
            if (oldOffset == UNINITIALIZED) {
                //这个等待不会持续很长时间
                LOG.warn("等待 block[{}] 初始化。。。。。。。", this);
                Thread.yield();
                continue;
            }
            if (oldOffset == OOM) {
                // doh we ran out of ram. return -1 to chuck this away.
                return -1;
            }

            if (oldOffset + size > data.length) {
                return -1; // alloc doesn't fit
            }
            // Try to atomically claim this block
            if (realDataSize.compareAndSet(oldOffset, oldOffset + size)) {
                // we got the alloc
                allocCount.incrementAndGet();
                return oldOffset;
            }
            // we raced and lost alloc, try again
        }
    }

    /**
     * 返回下次添加数据的偏移量
     */
    public int getDataOffsetInBlock() {
        return realDataSize.get();
    }

    /**
     * 添加一个packet的data数组到当前block
     *
     * @param pdata
     *
     * @return 插入的packet的offset
     */
    public int appendPacket(byte[] pdata) {
        Preconditions.checkNotNull(pdata, "packet data[{}]不应该是null！");
        return appendData(pdata);
    }

    /**
     * 在packetPos上填充packet,更新packet计数器
     *
     * @param packetData
     *
     * @return
     */
    private int appendData(byte[] packetData) {
        int alloc = alloc(packetData.length);
        if (alloc == -1) {
            //add exception here
            return -1;
        }
        System.arraycopy(packetData, 0, data, alloc, packetData.length);
        return alloc;
    }

    public byte[] getData() {
        return data;
    }

    public byte[] getRealData() {
        byte[] buf = new byte[realDataSize.get()];
        System.arraycopy(data, 0, buf, 0, buf.length);
        return buf;
    }

    /**
     * @return 当前block是否写满了
     */
    public boolean isBlockSpill() {
        return getDataOffsetInBlock() == getSize();
    }

    @Override
    public byte[] getBytes() {
        return data;
    }

    @Override
    public String toString() {
        return "Block{"
            + "defaulBlockSize=" + defaultBlockLen
            + ", size=" + size
            + ", id=" + id
            + ", offsetInFile=" + offsetInFile
            + ", realDataSize=" + realDataSize
            + ", allocCount=" + allocCount
            + '}';
    }
}
