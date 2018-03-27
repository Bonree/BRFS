package com.br.disknode.buf;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel.MapMode;

import com.br.disknode.utils.BufferUtils;

public class MappedWriteBuffer implements WriteBuffer {
	private MappedByteBuffer buffer;
	
	private RandomAccessFile file;
	private int capacity;
	
	private long filePosition;
	
	public MappedWriteBuffer(RandomAccessFile file, int capacity) throws IOException {
		this.file = file;
		this.capacity = capacity;
		this.filePosition = file.getFilePointer();
		resetBuffer();
	}
	
	private void resetBuffer() throws IOException {
		if(buffer != null) {
			BufferUtils.release(buffer);
		}
		buffer = file.getChannel().map(MapMode.READ_WRITE, filePosition, capacity);
	}

	@Override
	public int size() {
		return buffer.position();
	}

	@Override
	public int capacity() {
		return buffer.capacity();
	}
	
	@Override
	public int write(byte[] datas) {
		return write(datas, 0, datas.length);
	}

	@Override
	public int write(byte[] datas, int offset, int size) {
		int canWrite = Math.min(buffer.remaining(), size);
		
		buffer.put(datas, offset, canWrite);
		return canWrite;
	}

	@Override
	public void flush() throws IOException {
		buffer.force();
		filePosition += buffer.position();
		resetBuffer();
	}

	@Override
	public void setFilePosition(int position) throws IOException {
		filePosition = position;
		resetBuffer();
	}

}
