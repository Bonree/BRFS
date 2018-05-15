package com.bonree.brfs.client;

import java.io.Closeable;

public interface StorageNameStick extends Closeable {
	String[] writeData(InputItem[] itemArrays);
	String writeData(InputItem item);
	InputItem readData(String fid) throws Exception;
	boolean deleteData(long startTime, long endTime);
}
