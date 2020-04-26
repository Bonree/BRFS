package com.bonree.brfs.block.codec;

import com.bonree.brfs.block.codec.v1.DefaultBlockHeader;
import com.bonree.brfs.block.codec.v1.DefaultBlockTailer;
import com.bonree.brfs.common.proto.FileDataProtos.FileContent;

public interface BlockEncoder {
    DefaultBlockHeader.Builder newHeaderBuilder();

    DefaultBlockTailer.Builder newTailerBuilder();

    ByteHolder process(FileContent data) throws BlockDataProcessException;
}
