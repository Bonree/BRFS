package com.bonree.brfs.common.net.http.data;

import com.google.protobuf.InvalidProtocolBufferException;
import java.util.Arrays;
import java.util.Random;
import org.junit.Test;

/**
 * @author wangchao
 */
public class FSPacketTest {
    @Test
    public void testDatranspot() {
        Random random = new Random(10000);
        byte[] buf = new byte[512];
        random.nextBytes(buf);
        String fileName = "a.txt";
        int storageName = 0;
        long offset = 0;
        //**** client ***
        //构建一个数据包
        FSPacket fsPacket = new FSPacket(storageName, fileName, offset, 0, buf);
        //如果这是文件中最后一个数据包
        fsPacket.setLastPacketInFile();
        //构建真正的pb对象
        fsPacket.build();
        //将数据包序列化
        byte[] serialize = FSPacketUtil.serialize(fsPacket);

        // do somthing
        // ****** server *****
        try {
            FSPacket deserialze = FSPacketUtil.deserialize(serialize);
            assert fileName.equals(deserialze.getFileName());
            assert storageName == deserialze.getStorageName();
            assert offset == deserialze.getOffsetInFile();
            assert buf.length == deserialze.getData().length;
            assert Arrays.equals(buf, deserialze.getData());
        } catch (InvalidProtocolBufferException e) {
            System.out.println(e);
        }

    }
}
