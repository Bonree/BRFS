package com.bonree.brfs.common.net.tcp.client;

public interface ResponseHandler<T> {
    void handle(T response);

    void error(Throwable t);
}
