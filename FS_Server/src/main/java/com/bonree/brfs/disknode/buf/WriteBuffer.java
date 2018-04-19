package com.bonree.brfs.disknode.buf;

import java.io.IOException;

/**
 * 写入文件前对数据进行缓存的接口定义
 * 
 * {@link StreamWriteBuffer}
 * {@link MappedWriteBuffer}
 * 
 * @author chen
 *
 */
public interface WriteBuffer {
	/**
	 * 返回当前buffer中存储数据的长度
	 * 
	 * @return
	 */
	public int size();
	
	/**
	 * 当前buffer的数据容量大小
	 * 
	 * @return
	 */
	public int capacity();
	
	/**
	 * 设置缓存写入数据时文件指针的偏移量
	 * 
	 * @param position
	 * @throws IOException
	 */
	public void setFilePosition(int position) throws IOException;
	
	/**
	 * 写入指定字节数组中的所有数据
	 * 
	 * @param datas
	 * @return 实际写入的数据量
	 */
	public int write(byte[] datas);
	
	/**
	 * 写入指定字节数组中的一个片段
	 * 
	 * @param datas
	 * @param offset 写入数据在字节数组中的开始位置
	 * @param size 写入数据的长度
	 * @return 实际写入的数据量
	 */
	public int write(byte[] datas, int offset, int size);
	
	/**
	 * 同步数据到磁盘文件
	 * @throws IOException
	 */
	public void flush() throws IOException;
}
