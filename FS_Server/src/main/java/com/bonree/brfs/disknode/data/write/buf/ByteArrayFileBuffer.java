package com.bonree.brfs.disknode.data.write.buf;

import java.io.IOException;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public class ByteArrayFileBuffer implements FileBuffer {
	private byte[] byteArray;
	private volatile int position;
	
	public ByteArrayFileBuffer(int capacity) {
		this.byteArray = new byte[capacity];
		this.position = 0;
	}

	@Override
	public int readableSize() {
		return position;
	}

	@Override
	public int capacity() {
		return byteArray.length;
	}

	@Override
	public int writableSize() {
		return byteArray.length - position;
	}

	@Override
	public void write(byte[] datas) {
		write(datas, 0, datas.length);
	}

	@Override
	public void write(byte[] datas, int offset, int size) {
		if(size > writableSize()) {
			throw new BufferOverflowException();
		}
		
		System.arraycopy(datas, offset, byteArray, position, size);
		position += size;
	}

	@Override
	public void clear() {
		position = 0;
	}

	@Override
	public void flush(FileChannel channel) throws IOException {
		channel.write(ByteBuffer.wrap(byteArray, 0, position));
		channel.force(false);
	}

}
