package com.bonree.brfs.duplication.datastream.blockcache;

public interface BlockPoolInterface {
    void putbackBlocks(BlockInterface data);
    BlockInterface getBlock() throws InterruptedException;
}
