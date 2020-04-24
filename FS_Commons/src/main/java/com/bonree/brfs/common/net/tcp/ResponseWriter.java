package com.bonree.brfs.common.net.tcp;

public interface ResponseWriter<T> {

    void write(T response);
}
