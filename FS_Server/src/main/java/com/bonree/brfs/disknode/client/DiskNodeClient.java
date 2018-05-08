package com.bonree.brfs.disknode.client;

import java.io.Closeable;
import java.io.IOException;
import java.util.BitSet;
import java.util.List;

import com.bonree.brfs.disknode.server.handler.data.FileInfo;

public interface DiskNodeClient extends Closeable {
	boolean initFile(String path);
	WriteResult writeData(String path, int sequence, byte[] bytes) throws IOException;
	WriteResult writeData(String path, int sequence, byte[] bytes, int offset, int size) throws IOException;
	byte[] readData(String path, int offset, int size) throws IOException;
	boolean closeFile(String path);
	List<FileInfo> listFiles(String path, int level);
	boolean deleteFile(String path, boolean force);
	boolean deleteDir(String path, boolean force, boolean recursive);
	
	/**
	 * 返回当前节点的写入序列号
	 * 
	 * @param path
	 * @return 序列号数组，已按升序排列
	 */
	BitSet getWritingSequence(String path);
	
	void recover(String path, SeqInfoList infos) throws IOException;
	
	byte[] getBytesBySequence(String path, int sequence);
	
	void copyFrom(String host, int port, String remotePath, String localPath) throws IOException;
	void copyTo(String host, int port, String localPath, String remotePath) throws IOException;
}
