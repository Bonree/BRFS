package com.bonree.brfs.duplication.datastream.blockcache;

public interface BlockInterface {
    long getSize();

    int getId();

    void reset();

    void init();

    byte[] getRealData();

    int getDataOffsetInBlock();

    int appendPacket(byte[] pData);

    boolean isBlockSpill();
}
