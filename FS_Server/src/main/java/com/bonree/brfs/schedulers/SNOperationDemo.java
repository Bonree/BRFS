package com.bonree.brfs.schedulers;

import java.io.IOException;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

import com.bonree.brfs.disknode.client.WriteResult;

public class SNOperationDemo implements SNOperation {

	@Override
	public boolean initFile(String path, boolean override) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public WriteResult writeData(String path, int sequence, byte[] bytes) throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public WriteResult writeData(String path, int sequence, byte[] bytes, int offset, int size) throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public byte[] readData(String path, int offset, int size) throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean closeFile(String path) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean deleteFile(String path, boolean force) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean deleteDir(String path, boolean force, boolean recursive) {
		// TODO Auto-generated method stub
		return true;
	}

	@Override
	public int getValidLength(String path) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public BitSet getWritingSequence(String path) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void copyFrom(String host, int port, String from, String to) {
		// TODO Auto-generated method stub

	}

	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public List<String> getFiles(String path) {
		List<String> files = new ArrayList<String>();
		files.add("a");
		files.add("b");
		return files;
	}

	@Override
	public boolean checkCRCFile(String path) {
		
		return true;
	}

}
