package com.bonree.brfs.duplication.storagename;

import com.bonree.brfs.server.sequence.StorageSequenceGenetor;

public class ZkStorageIdBuilder implements StorageIdBuilder {
	private StorageSequenceGenetor generator;
	
	public ZkStorageIdBuilder(String zkAddrsss, String seqPath) {
		generator = StorageSequenceGenetor.getInstance(zkAddrsss, seqPath);
	}

	@Override
	public short createStorageId() {
		return generator.getIncreSequence().shortValue();
	}

}
