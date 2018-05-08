package com.bonree.brfs.disknode.data.write.worker;

import java.util.List;
import java.util.Random;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RandomWriteWorkerSelector implements WriteWorkerSelector {
	private static final Logger Log = LoggerFactory.getLogger(RandomWriteWorkerSelector.class);
	
	private static Random rand = new Random(System.currentTimeMillis());

	@Override
	public WriteWorker select(List<WriteWorker> workers) {
		Log.info("Select a worker from {} workers", workers.size());
		return workers.get(rand.nextInt(workers.size()));
	}

}
