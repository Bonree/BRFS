package com.bonree.brfs.duplication.datastream.blockcache;

public interface BlockPool {
    void putbackBlocks(Block data);
    Block getBlock() throws InterruptedException;
}
