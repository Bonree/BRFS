package com.bonree.brfs.disknode.buf;

import java.io.IOException;

public interface WriteBuffer {
	public int size();
	public int capacity();
	public void setFilePosition(int position) throws IOException;
	public int write(byte[] datas);
	public int write(byte[] datas, int offset, int size);
	public void flush() throws IOException;
}
