package com.bonree.brfs.common.exception;

public class ConfigParseException extends Exception {

    private static final long serialVersionUID = 3404439704959289003L;

    public ConfigParseException() {
        super();
    }

    public ConfigParseException(String s) {
        super(s);
    }

    public ConfigParseException(String message, Throwable cause) {
        super(message, cause);
    }

    public ConfigParseException(Throwable cause) {
        super(cause);
    }

}
