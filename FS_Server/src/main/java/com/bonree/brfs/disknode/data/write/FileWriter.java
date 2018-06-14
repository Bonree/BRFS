package com.bonree.brfs.disknode.data.write;

import java.io.Closeable;
import java.io.IOException;

/**
 * 磁盘文件写入接口类
 * 
 * @author yupeng
 *
 */
public interface FileWriter extends Closeable {
	/**
	 * 获取当前写入文件的文件名
	 * 
	 * @return
	 */
	String getPath();
	
	/**
	 * 当前文件写入偏移量
	 * 
	 * @return
	 */
	long position();
	
	/**
	 * 设置文件写入偏移量，下次写入就从这个
	 * 位置开始
	 * 
	 * @param pos
	 * @throws IOException 
	 */
	void position(long pos) throws IOException;
	
	/**
	 * 写入指定字节数据到文件
	 * 
	 * @param bytes
	 */
	void write(byte[] bytes) throws IOException;
	
	/**
	 * 写入指定字节段到文件
	 * 
	 * @param bytes
	 * @param offset
	 * @param size
	 */
	void write(byte[] bytes, int offset, int size) throws IOException;
	
	/**
	 * 将数据同步到磁盘
	 * 
	 * @throws IOException 
	 */
	void flush() throws IOException;
}
