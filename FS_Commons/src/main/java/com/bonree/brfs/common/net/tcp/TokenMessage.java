package com.bonree.brfs.common.net.tcp;

public class TokenMessage<T> {
    private final int token;
    private final T msg;

    public TokenMessage(int token, T msg) {
        this.token = token;
        this.msg = msg;
    }

    public int messageToken() {
        return token;
    }

    public T message() {
        return msg;
    }
}
