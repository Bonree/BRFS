package com.bonree.brfs.disknode.client;

import java.io.Closeable;
import java.io.IOException;
import java.util.BitSet;

public interface DiskNodeClient extends Closeable {
	boolean initFile(String path, boolean override);
	WriteResult writeData(String path, int sequence, byte[] bytes) throws IOException;
	WriteResult writeData(String path, int sequence, byte[] bytes, int offset, int size) throws IOException;
	byte[] readData(String path, int offset, int size) throws IOException;
	boolean closeFile(String path);
	boolean deleteFile(String path, boolean force);
	boolean deleteDir(String path, boolean force, boolean recursive);
	int getValidLength(String path);
	
	/**
	 * 返回当前节点的写入序列号
	 * 
	 * @param path
	 * @return 序列号数组，已按升序排列
	 */
	BitSet getWritingSequence(String path);
	
	void copyFrom(String host, int port, String from, String to);
}
