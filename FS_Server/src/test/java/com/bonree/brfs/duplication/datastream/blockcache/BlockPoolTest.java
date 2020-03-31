package com.bonree.brfs.duplication.datastream.blockcache;

import com.bonree.brfs.common.net.http.data.FSPacket;
import com.bonree.brfs.common.utils.Bytes;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;

/**
 * Test the {@link BlockPool} class
 * @author wangchao
 * @date 2020/3/20 - 2:48 下午
 */
public class BlockPoolTest {
    public static final int blockSize = 64*1024;
    ArrayList<FSPacket> fsPackets = new ArrayList<>();
    @Test
    public void someCK() throws InterruptedException {
        BlockPool blockPool = new BlockPool(blockSize,2, 0);
        byte[] packet = "abcd".getBytes();

        Block block = blockPool.getBlock();
        //在使用的时候才分配实际存储空间,delay
        // block.init();
        assert (block.realDataSize.get()==0);
        int offset = block.appendPacket(packet,0,true);
        assert (offset == 0);
        assert (block.getData().length == blockSize);
        assert Bytes.startWith(block.getData(),packet);
        assert block.getDataOffsetInBlock() == packet.length;
        assert block.getRealData().length==packet.length;
        int blockId = block.getId();
        //回收
        blockPool.putbackBlocks(block);
        assert (blockPool.getReclaimedBlocks().size() == 1);
        assert Bytes.startWith(blockPool.getReclaimedBlocks().peek().getData(),packet);

        packet = "cdef".getBytes();
        //重复利用
        Block block1 = blockPool.getBlock();
        //检查是不是刚才还回去的
        Assert.assertEquals(blockId,block1.getId());
        offset = block1.appendPacket(packet,64,true);
        //检查是不是reset了
        assert (offset == 0);
        assert (block.getData().length == blockSize);
        assert Bytes.startWith(block.getData(),packet);
        assert block.getDataOffsetInBlock() == packet.length;
    }
    @Test
    public void useCase() throws InterruptedException {
        BlockPool blockPool = new BlockPool(blockSize,2, 0);

        //收到packet，解析出data bytes，64
        byte[] packet = new byte[64];
        //申请一个block
        Block block = blockPool.getBlock();
        //每次append 的时候，检查（offset+packet.length）< 64k，判断是否继续写入
        //更新延迟时间
        //如果等于64k，写入这个block brfs dn
        //如果大于64k，那么之前的流程有问题
        int offset = block.appendPacket(packet,0,true);
        assert((offset + packet.length)==(64)) ;
    }

    @Test
    public void testFullPool() throws InterruptedException {
        BlockPool blockPool = new BlockPool(blockSize,1, 0);
        Block block = blockPool.getBlock();
        Block block2 = blockPool.getBlock();
        // 日志中应打印等待信息
        // 过了3秒 返回block2 为null ，意为经过等待没有获得block
        Assert.assertNull(block2);
        //设置一个线程 2秒后把第一个block归还
        new Thread(()->{
            try {
                Thread.sleep(2000);
                blockPool.putbackBlocks(block);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }).start();
        //再次尝试获取，这次在等待过程中可以别的线程会归还block
        block2 = blockPool.getBlock();
        //日志中应打印等待信息
        Assert.assertNotNull(block2);

    }
}
