package com.bonree.brfs.block.codec;

public interface FileCodec<D> {
    void write(D data);

    D read(long offset, int size);
}
