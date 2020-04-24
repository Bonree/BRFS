package com.bonree.brfs.duplication.datastream.dataengine.impl;

import java.util.concurrent.LinkedBlockingQueue;

public class BlockingQueueDataPool implements DataPool {
    private LinkedBlockingQueue<DataObject> datas;

    public BlockingQueueDataPool(int capacity) {
        this.datas = new LinkedBlockingQueue<DataObject>(capacity);
    }

    @Override
    public int size() {
        return datas.size();
    }

    @Override
    public boolean isEmpty() {
        return datas.isEmpty();
    }

    @Override
    public void put(DataObject data) throws InterruptedException {
        datas.put(data);
    }

    @Override
    public DataObject take() throws InterruptedException {
        return datas.take();
    }

    @Override
    public DataObject peek() {
        return datas.peek();
    }

    @Override
    public void remove() {
        datas.remove();
    }

    @Override
    public void clear() {
        datas.clear();
    }
}
