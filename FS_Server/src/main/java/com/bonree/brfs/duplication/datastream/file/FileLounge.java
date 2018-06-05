package com.bonree.brfs.duplication.datastream.file;

import java.util.List;

/**
 * 
 * 文件节点元数据信息的维护类
 * 
 * @author yupeng
 *
 */
public interface FileLounge {
	/**
	 * 通过外部添加文件，只在文件转移的时候会发生
	 * 
	 * @param file
	 */
	void addFileLimiter(FileLimiter file);
	
	/**
	 * 获取能容纳指定大小数据的文件节点
	 * 
	 * @param size 需要的空闲空间大小
	 * @return 可用的文件节点
	 */
	FileLimiter getFileLimiter(int size);
	
	/**
	 * <p>为一组数据分配文件节点。</p>
	 * 
	 * <p>输入参数为数据的字节大小数组，会根据各个数据大小分别为
	 * 它们分配可用的文件节点，然后以文件列表的方式返回。文件列表
	 * 的大小和字节大小数组的长度相同，并且是一一对应的。<p>
	 * 
	 * <p>对于多条数据，之所以没有一次性申请所有数据大小总和的空
	 * 间，是因为可能一个文件无法容纳所有的数据，多条数据可能分布
	 * 在不同的文件中，因此分别申请能达到最大的灵活性</p>
	 * 
	 * @param requestSizes 需要申请的字节大小数组
	 * @return
	 */
	FileLimiter[] getFileLimiterList(int[] requestSizes);
	
	/**
	 * 清空当前所有文件节点
	 */
	void clean();
	
	/**
	 * 获取当前所有的文件节点
	 * 
	 * @return
	 */
	List<FileLimiter> listFileLimiters();
	
	/**
	 * 设置文件关闭事件监听接口，当文件关闭时可以做一些额外操作
	 * 
	 * @param listener
	 */
	void setFileCloseListener(FileCloseListener listener);
}
