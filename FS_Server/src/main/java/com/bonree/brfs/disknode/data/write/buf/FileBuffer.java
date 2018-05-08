package com.bonree.brfs.disknode.data.write.buf;

import java.io.IOException;
import java.nio.channels.FileChannel;

/**
 * 写入文件前对数据进行缓存的接口定义
 * 
 * @author yupeng
 *
 */
public interface FileBuffer {
	/**
	 * 返回当前buffer中存储数据的长度
	 * 
	 * @return
	 */
	int size();
	
	/**
	 * 当前buffer的数据容量大小
	 * 
	 * @return
	 */
	int capacity();
	
	/**
	 * 当前缓存剩余的空间大小
	 * 
	 * @return 可写入字节数
	 */
	int remaining();
	
	/**
	 * 写入指定字节数组中的所有数据
	 * 
	 * @param datas
	 */
	void write(byte[] datas);
	
	/**
	 * 写入指定字节数组中的一个片段
	 * 
	 * @param datas
	 * @param offset 写入数据在字节数组中的开始位置
	 * @param size 写入数据的长度
	 */
	void write(byte[] datas, int offset, int size);
	
	/**
	 * 清空缓存数据
	 */
	void clear();
	
	/**
	 * 同步数据到磁盘文件
	 * 
	 * @throws IOException
	 */
	void flush(FileChannel channel) throws IOException;
}
