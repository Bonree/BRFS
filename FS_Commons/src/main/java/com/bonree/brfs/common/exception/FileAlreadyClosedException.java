package com.bonree.brfs.common.exception;

public class FileAlreadyClosedException extends RuntimeException {

    public FileAlreadyClosedException(String message) {
        super(message);
    }
}
