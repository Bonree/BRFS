package com.bonree.brfs.client.data.read.connection;

public interface DataRequestHandler<T> {
    T handle(DataConnection connection) throws Exception;
}
