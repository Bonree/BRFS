package com.bonree.brfs.duplication.datastream.dataengine.impl;


public class BlockingQueueDataPoolFactory implements DataPoolFactory {
	private final int poolCapacity;
	
	public BlockingQueueDataPoolFactory(int capacity) {
		this.poolCapacity = capacity;
	}

	@Override
	public DataPool createDataPool() {
		return new BlockingQueueDataPool(poolCapacity);
	}

}
