package com.bonree.brfs.duplication.datastream.writer;

public interface StorageRegionWriteCallback {
	void complete(String[] fids);
	void complete(String fid);
	void error();
}
