package com.bonree.brfs.client;

public interface StorageNameStick {
	String[] writeData(InputItem[] itemArrays);
	String writeData(InputItem item);
	InputItem readData(String fid) throws Exception;
	boolean deleteData(long startTime, long endTime);
}
