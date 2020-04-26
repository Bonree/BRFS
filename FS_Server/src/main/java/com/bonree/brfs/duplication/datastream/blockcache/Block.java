package com.bonree.brfs.duplication.datastream.blockcache;

public interface Block {
    long getSize();

    int getId();

    void reset();

    void init();

    byte[] getRealData();

    int getDataOffsetInBlock();

    int appendPacket(byte[] packetData);

    boolean isBlockSpill();
}
