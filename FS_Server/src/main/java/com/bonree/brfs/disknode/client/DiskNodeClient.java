package com.bonree.brfs.disknode.client;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

import com.bonree.brfs.disknode.server.handler.data.FileInfo;
import com.bonree.brfs.disknode.server.handler.data.WriteData;
import com.bonree.brfs.disknode.server.handler.data.WriteResult;

public interface DiskNodeClient extends Closeable {
	boolean ping();
	
	long openFile(String path, long capacity);
	long closeFile(String path);
	
	WriteResult writeData(String path, byte[] bytes) throws IOException;
	WriteResult writeData(String path, byte[] bytes, int offset, int size) throws IOException;
	
	WriteResult[] writeDatas(String path, WriteData[] dataList) throws IOException;
	
	boolean flush(String file) throws IOException;
	
	byte[] readData(String path, long offset) throws IOException;
	byte[] readData(String path, long offset, int size) throws IOException;
	
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
	
	boolean recover(String path, long fileLength, List<String> serviceList);
	
	void copyFrom(String host, int port, String remotePath, String localPath) throws Exception;
	void copyTo(String host, int port, String localPath, String remotePath) throws Exception;
}
