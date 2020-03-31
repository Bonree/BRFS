package com.bonree.brfs.duplication.datastream.blockcache;

import com.bonree.brfs.common.net.http.data.FSPacket;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Random;

/**
 * @author wangchao
 * @date 2020/3/27 - 11:08 上午
 */
public class BlockTest{
    static Random random = new Random(System.currentTimeMillis());
    @Test
    public void parallelAppend(){
        //模拟追加最后一个pacekt ,最后一个数据只有4byte
        int blocksize = 64 * 1024; //每个block存1024个
        int fileLen = 132;//64+64+4
        int pLen = 64;
        int pSize = fileLen / pLen + 1;
        int storage = 0000;
        String file;
        boolean isLast = true;
        int pacPos;//packet 在block中的offset
        Block block = new Block(0, blocksize);
        block.init();
        FSPacket fsPacket;
        ArrayList<FSPacket> packets = new ArrayList<>();
        byte[] data = new byte[fileLen%pLen];;

        file = "file:" + random.nextInt();

        for (int i = pSize; i > 0; i--) {

            fsPacket = new FSPacket(storage, file, (i-1) * pLen, i, data);
            if (isLast) fsPacket.setLastPacketInFile();
            fsPacket.build();
            Assert.assertEquals(fsPacket.getData().length,data.length);
            pacPos = fsPacket.getPacPos(block.getSize());
            //i*plen=pkt offset in file
            Assert.assertEquals(pacPos, ((i-1)*pLen)% blocksize);
            Assert.assertTrue(!block.checkBit(pacPos));

            block.appendPacket(fsPacket.getData(), pacPos, isLast);
            //最后一个包最先发，发完将其他包标志位清除
            isLast = false;
            data = new byte[pLen];
            Assert.assertTrue(block.packetExist(pacPos));

        }

    }

    @Test
    public void serialAppend() {
        //模拟追加一个pacekt ,最后一个数据只有4byte
        int blocksize = 64 * 1024; //每个block存1024个
        int fileLen = 132;
        int pLen = 64;
        int pSize = fileLen / pLen + 1;
        int storage = 0000;
        String file;
        boolean isLast = false;
        int pacPos;//packet 在block中的offset
        Block block = new Block(0, blocksize);
        block.init();
        FSPacket fsPacket;
        file = "file:" + random.nextInt();
        byte[] data ;

        for (int i = 0; i < pSize; i++) {

            if (i == pSize - 1) {
                isLast = true;
                data = new byte[fileLen % pLen];
            }else {
                data = new byte[pLen];
            }
            fsPacket = new FSPacket(storage, file, i * pLen, i, data);
            if (isLast) fsPacket.setLastPacketInFile();
            fsPacket.build();
            Assert.assertEquals(fsPacket.getData().length,data.length);
            pacPos = fsPacket.getPacPos(block.getSize());
            //i*plen=pkt offset in file
            Assert.assertEquals(pacPos, (i*pLen)% blocksize);
            Assert.assertTrue(!block.checkBit(pacPos));
            block.appendPacket(fsPacket.getData(), pacPos, isLast);
            Assert.assertTrue(block.packetExist(pacPos));

        }

    }
    @Test
    public void testBitMap(){
        int blocksize = 64 * 1024;
        int plen = blocksize/1024;
        Block block = new Block(1, blocksize);
        block.init();
        block.setBit(0);
        assert block.checkBit(0);
        //设置第二个pacekt
        block.setBit(1);
        //检查偏移量为64的pcket
        assert block.packetExist(64);
        Assert.assertEquals(2,block.getAcceptedPacketCount());


        block.setBit(2);
        block.setBit(3);
        block.setBit(4);
        block.setBit(5);
        block.setBit(6);
        block.setBit(7);
        block.setBit(8);
        block.setExpectPacketSize(8);
        Assert.assertEquals(9,block.getAcceptedPacketCount());
//        Assert.assertTrue(block.isAcceptedAllPacket());
    }

}
