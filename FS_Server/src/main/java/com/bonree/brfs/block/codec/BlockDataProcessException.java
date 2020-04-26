package com.bonree.brfs.block.codec;

public class BlockDataProcessException extends Exception {
    private static final long serialVersionUID = -6260368036840699317L;

    public BlockDataProcessException(String message) {
        super(message);
    }

    public BlockDataProcessException(String message, Throwable cause) {
        super(message, cause);
    }
}
