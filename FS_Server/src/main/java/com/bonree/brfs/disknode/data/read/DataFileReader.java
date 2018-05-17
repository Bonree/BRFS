package com.bonree.brfs.disknode.data.read;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel.MapMode;

import com.bonree.brfs.common.utils.CloseUtils;
import com.bonree.brfs.disknode.utils.BufferUtils;

public class DataFileReader {
	
	private DataFileReader() {}
	
	public static byte[] readFile(File file, int offset, int size) {
		return readFile(file.getAbsolutePath(), offset, size);
	}
	
	public static byte[] readFile(String filePath, int offset, int size) {
		RandomAccessFile file = null;
		MappedByteBuffer buffer = null;
		try {
			file = new RandomAccessFile(filePath, "r");
			long fileLength = file.length();
			
			if(offset >= fileLength) {
				return null;
			}
			
			offset = Math.max(0, offset);
			size = (int) Math.min(size, file.length() - offset);
			byte[] bytes = new byte[size];
			buffer = file.getChannel().map(MapMode.READ_ONLY, offset, size);
			buffer.get(bytes);
			
			return bytes;
		} catch (IOException e) {
		} finally {
			CloseUtils.closeQuietly(file);
			BufferUtils.release(buffer);
		}
		
		return null;
	}
}
