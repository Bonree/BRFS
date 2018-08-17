package com.bonree.brfs.disknode.client;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

import com.bonree.brfs.disknode.server.handler.data.FileInfo;

public interface DiskNodeClient extends Closeable {
	boolean ping();
	
	long openFile(String path, long capacity);
	long closeFile(String path);
	
	WriteResult writeData(String path, byte[] bytes) throws IOException;
	WriteResult writeData(String path, byte[] bytes, int offset, int size) throws IOException;
	
	WriteResult[] writeDatas(String path, List<byte[]> dataList) throws IOException;
	
	boolean flush(String file) throws IOException;
	
	void readData(String path, long offset, ByteConsumer consumer) throws IOException;
	void readData(String path, long offset, int size, ByteConsumer consumer) throws IOException;
	
	void readFile(String path, ByteConsumer consumer) throws IOException;
	
	List<FileInfo> listFiles(String path, int level);
	boolean deleteFile(String path, boolean force);
	boolean deleteDir(String path, boolean force, boolean recursive);
	
	/**
	 * 返回当前节点的写入序列号
	 * 
	 * @param path
	 * @return 序列号数组，已按升序排列
	 */
	long getFileLength(String path);
	
	boolean recover(String path, long fileLength, List<String> fullstates);
	
	public static interface ByteConsumer {
		void consume(byte[] bytes, boolean endOfConsume);
		void error(Throwable e);
	}
}
