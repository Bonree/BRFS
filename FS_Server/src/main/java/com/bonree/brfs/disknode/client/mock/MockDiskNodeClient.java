package com.bonree.brfs.disknode.client.mock;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.List;

import com.bonree.brfs.disknode.client.DiskNodeClient;
import com.bonree.brfs.disknode.client.RecoverInfo;
import com.bonree.brfs.disknode.server.handler.data.FileInfo;
import com.bonree.brfs.disknode.server.handler.data.WriteData;
import com.bonree.brfs.disknode.server.handler.data.WriteResult;

public class MockDiskNodeClient implements DiskNodeClient {

	@Override
	public void close() throws IOException {
	}

	@Override
	public WriteResult writeData(String path, int sequence, byte[] bytes)
			throws IOException {
		WriteResult result = new WriteResult();
		result.setSequence(sequence);
		result.setSize(bytes.length);
		
		return result;
	}

	@Override
	public WriteResult writeData(String path, int sequence, byte[] bytes,
			int offset, int size) throws IOException {
		WriteResult result = new WriteResult();
		result.setSequence(sequence);
		result.setSize(size);
		
		return result;
	}

	@Override
	public WriteResult[] writeDatas(String path, WriteData[] dataList)
			throws IOException {
		WriteResult[] results = new WriteResult[dataList.length];
		for(int i = 0 ; i < dataList.length; i++) {
			results[i] = new WriteResult();
			results[i].setSequence(dataList[i].getDiskSequence());
			results[i].setSize(dataList[i].getBytes().length);
		}
		
		return results;
	}

	@Override
	public byte[] readData(String path, int offset, int size)
			throws IOException {
		return new byte[13];
	}

	@Override
	public boolean closeFile(String path) {
		return true;
	}

	@Override
	public List<FileInfo> listFiles(String path, int level) {
		return new ArrayList<FileInfo>();
	}

	@Override
	public boolean deleteFile(String path, boolean force) {
		return true;
	}

	@Override
	public boolean deleteDir(String path, boolean force, boolean recursive) {
		return true;
	}

	@Override
	public BitSet getWritingSequence(String path) {
		return new BitSet();
	}

	@Override
	public int[] getWritingFileMetaInfo(String path) {
		int[] result = new int[2];
		Arrays.fill(result, 0);
		
		return new int[2];
	}

	@Override
	public boolean recover(String path, RecoverInfo infos) {
		return true;
	}

	@Override
	public byte[] getBytesBySequence(String path, int sequence) {
		return new byte[13];
	}

	@Override
	public void copyFrom(String host, int port, String remotePath,
			String localPath) throws Exception {
	}

	@Override
	public void copyTo(String host, int port, String localPath,
			String remotePath) throws Exception {
	}

}
