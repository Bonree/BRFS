package com.br.disknode.buf;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;


public class SimpleWriteBuffer implements WriteBuffer {
	private RandomAccessFile file;
	
	private ByteArrayOutputStream buf;
	private int capacity;
	
	public SimpleWriteBuffer(RandomAccessFile file, int capacity) {
		this.file = file;
		this.capacity = capacity;
		buf = new ByteArrayOutputStream(capacity);
	}

	@Override
	public int size() {
		return buf.size();
	}

	@Override
	public int capacity() {
		return capacity;
	}
	
	@Override
	public int write(byte[] datas) {
		return write(datas, 0, datas.length);
	}

	@Override
	public int write(byte[] datas, int offset, int size) {
		int canWrite = Math.min(size, capacity - size());
		
		buf.write(datas, offset, canWrite);
		return canWrite;
	}

	@Override
	public void flush() throws IOException {
		file.write(buf.toByteArray());
		file.getFD().sync();
		buf.reset();
	}

	@Override
	public void setFilePosition(int position) throws IOException {
		file.seek(position);
	}
}
