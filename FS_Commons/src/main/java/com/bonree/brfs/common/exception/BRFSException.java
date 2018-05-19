package com.bonree.brfs.common.exception;

public class BRFSException extends RuntimeException {

    private static final long serialVersionUID = -4487368952738229999L;

    public BRFSException() {
        super();
    }

    public BRFSException(String s) {
        super(s);
    }

    public BRFSException(String message, Throwable cause) {
        super(message, cause);
    }

    public BRFSException(Throwable cause) {
        super(cause);
    }

}
