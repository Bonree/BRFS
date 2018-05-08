package com.bonree.brfs.disknode.data.read;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel.MapMode;

import com.bonree.brfs.disknode.utils.BufferUtils;

public class DataFileReader implements Closeable {
	private RandomAccessFile accessFile;
	
	private MappedByteBuffer buffer;
	
	public DataFileReader(String filePath) throws IOException {
		this(new File(filePath));
	}
	
	public DataFileReader(File file) throws IOException {
		accessFile = new RandomAccessFile(file, "r");
		buffer = accessFile.getChannel().map(MapMode.READ_ONLY, 0, file.length());
	}
	
	public byte[] read(int offset, int size) {
		if(offset < 0 || offset > buffer.capacity()) {
			return new byte[0];
		}
		
		int byteLen = Math.min(size, buffer.capacity() - offset);
		byte[] result = new byte[byteLen];
		buffer.position(offset);
		buffer.get(result);
		
		return result;
	}
	
	@Override
	public void close() throws IOException {
		accessFile.close();
		BufferUtils.release(buffer);
	}
}
