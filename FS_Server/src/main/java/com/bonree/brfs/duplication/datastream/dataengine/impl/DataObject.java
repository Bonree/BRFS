package com.bonree.brfs.duplication.datastream.dataengine.impl;

public interface DataObject {
    byte[] getBytes();

    int length();

    void processComplete(String result);
}
