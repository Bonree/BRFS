package com.bonree.brfs.duplication.datastream.file;

/**
 * 文件节点关闭的通知类
 * 
 * @author yupeng
 *
 */
public interface FileCloseListener {
	/**
	 * 文件需要进行关闭
	 * 
	 * @param file
	 * @throws Exception 
	 */
	void close(FileLimiter file) throws Exception;
}
