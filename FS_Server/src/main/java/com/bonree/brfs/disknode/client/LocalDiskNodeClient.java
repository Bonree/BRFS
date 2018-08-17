package com.bonree.brfs.disknode.client;

import java.io.IOException;
import java.util.List;

import com.bonree.brfs.disknode.server.handler.data.FileInfo;

public class LocalDiskNodeClient implements DiskNodeClient {

	@Override
	public WriteResult writeData(String path, byte[] bytes)
			throws IOException {
		return null;
	}

	@Override
	public WriteResult writeData(String path, byte[] bytes,
			int offset, int size) throws IOException {
		return null;
	}

	@Override
	public long closeFile(String path) {
		return -1;
	}

	@Override
	public List<FileInfo> listFiles(String path, int level) {
		return null;
	}

	@Override
	public boolean deleteFile(String path, boolean force) {
		return false;
	}

	@Override
	public boolean deleteDir(String path, boolean force, boolean recursive) {
		return false;
	}
	
	@Override
	public void close() throws IOException {
	}

	@Override
	public WriteResult[] writeDatas(String path, List<byte[]> dataList)
			throws IOException {
		return null;
	}

	@Override
	public boolean ping() {
		return true;
	}

	@Override
	public boolean flush(String file) throws IOException {
		return true;
	}

	@Override
	public long openFile(String path, long capacity) {
		return capacity;
	}

	@Override
	public void readData(String path, long offset, ByteConsumer consumer) throws IOException {
	}

	@Override
	public void readData(String path, long offset, int size, ByteConsumer consumer) throws IOException {
	}

	@Override
	public long getFileLength(String path) {
		return 0;
	}

	@Override
	public boolean recover(String path, long fileLength, List<String> fulls) {
		return false;
	}

	@Override
	public void readFile(String path, ByteConsumer consumer) {
		// TODO Auto-generated method stub
		
	}

}
