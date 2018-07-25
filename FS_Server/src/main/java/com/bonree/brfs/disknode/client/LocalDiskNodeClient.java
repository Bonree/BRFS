package com.bonree.brfs.disknode.client;

import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.List;

import com.bonree.brfs.common.utils.CloseUtils;
import com.bonree.brfs.disknode.data.read.DataFileReader;
import com.bonree.brfs.disknode.server.handler.data.FileInfo;
import com.bonree.brfs.disknode.server.handler.data.WriteData;
import com.bonree.brfs.disknode.server.handler.data.WriteResult;

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
	public void copyFrom(String host, int port, String remotePath,
			String localPath) throws Exception {
		DiskNodeClient client = null;
		BufferedOutputStream output = null;
		int bufferSize = 5 * 1024 * 1024;
		try {
			client = new HttpDiskNodeClient(host, port);
			byte[] bytes = client.readData(remotePath, 0, Integer.MAX_VALUE);
			output = new BufferedOutputStream(new FileOutputStream(localPath), bufferSize);
			output.write(bytes);
			output.flush();
		} finally {
			CloseUtils.closeQuietly(client);
			CloseUtils.closeQuietly(output);
		}
	}
	public boolean isExists(String host, int port,String remotePath) throws IOException {
		DiskNodeClient client = null;
		try {
			client = new HttpDiskNodeClient(host, port);
			byte[] bytes = client.readData(remotePath, 0, Integer.MAX_VALUE);
			if(bytes == null || bytes.length == 0) {
				return false;
			}
		} finally {
			CloseUtils.closeQuietly(client);
		}
		return true;
	}

	@Override
	public void copyTo(String host, int port, String localPath, String remotePath) throws Exception {
		DiskNodeClient client = null;
		int bufferSize = 5 * 1024 * 1024;
		try {
			client = new HttpDiskNodeClient(host, port);
			
			byte[] buf;
			int offset = 0;
			while((buf = DataFileReader.readFile(localPath, offset, bufferSize)).length != 0) {
				client.writeData(remotePath, buf);
				offset += buf.length;
			}
			
			client.closeFile(remotePath);
		} finally {
			CloseUtils.closeQuietly(client);
		}
	}
	
	@Override
	public void close() throws IOException {
	}

	@Override
	public WriteResult[] writeDatas(String path, WriteData[] dataList)
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
	public byte[] readData(String path, long offset) throws IOException {
		return null;
	}

	@Override
	public byte[] readData(String path, long offset, int size)
			throws IOException {
		return null;
	}

	@Override
	public long getFileLength(String path) {
		return 0;
	}

	@Override
	public boolean recover(String path, long fileLength, List<String> fulls) {
		return false;
	}

}
