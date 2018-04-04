package com.bonree.brfs.disknode.client;

import java.io.Closeable;
import java.io.IOException;

public interface DiskNodeClient extends Closeable {
	boolean initFile(String path, boolean override);
	WriteResult writeData(String path, byte[] bytes) throws IOException;
	WriteResult writeData(String path, byte[] bytes, int offset, int size) throws IOException;
	byte[] readData(String path, int offset, int size) throws IOException;
	boolean closeFile(String path);
	boolean deleteFile(String path);
	boolean deleteDir(String path, boolean recursive);
	int getValidLength(String path);
}
