package com.bonree.brfs.duplication.datastream.dataengine.impl;

public interface DataPool {
    int size();

    boolean isEmpty();

    void put(DataObject data) throws InterruptedException;

    DataObject take() throws InterruptedException;

    DataObject peek();

    void remove();

    void clear();
}
