package com.bonree.brfs.duplication.datastream.dataengine.impl;

import com.bonree.brfs.configuration.Configs;
import com.bonree.brfs.configuration.units.RegionNodeConfigs;

public class BlockingQueueDataPoolFactory implements DataPoolFactory {
	private final int poolCapacity;
	
	public BlockingQueueDataPoolFactory() {
	    this(Configs.getConfiguration().GetConfig(RegionNodeConfigs.CONFIG_DATA_POOL_CAPACITY));
	}
	
	public BlockingQueueDataPoolFactory(int capacity) {
		this.poolCapacity = capacity;
	}

	@Override
	public DataPool createDataPool() {
		return new BlockingQueueDataPool(poolCapacity);
	}

}
