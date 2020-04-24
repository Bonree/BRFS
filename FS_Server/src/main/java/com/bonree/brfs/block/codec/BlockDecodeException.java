package com.bonree.brfs.block.codec;

public class BlockDecodeException extends Exception {
    private static final long serialVersionUID = 266049507578783277L;

    public BlockDecodeException(String message) {
        super(message);
    }

    public BlockDecodeException(String message, Throwable cause) {
        super(message, cause);
    }
}
