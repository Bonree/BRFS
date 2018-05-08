package com.bonree.brfs.disknode.data.write.buf;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * 数据写入文件前的缓存类
 * 
 * @author yupeng
 *
 */
public class ByteFileBuffer implements FileBuffer {
	private ByteBuffer buffer;
	
	public ByteFileBuffer(int capacity) {
		this.buffer = ByteBuffer.allocate(capacity);
	}
	
	@Override
	public int capacity() {
		return buffer.capacity();
	}
	
	@Override
	public int size() {
		return buffer.position();
	}
	
	@Override
	public int remaining() {
		return buffer.remaining();
	}
	
	@Override
	public void write(byte[] bytes) {
		write(bytes);
	}
	
	@Override
	public void write(byte[] bytes, int offset, int length) {
		buffer.put(bytes, offset, length);
	}
	
	@Override
	public void clear() {
		buffer.clear();
	}
	
	@Override
	public void flush(FileChannel channel) throws IOException {
		buffer.flip();
		channel.write(buffer);
		channel.force(false);
	}
}
