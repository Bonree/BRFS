package com.bonree.brfs.duplication.coordinator;

import com.bonree.brfs.common.process.LifeCycle;

/**
 * 管理{@link FileNodeSink}的接口
 * 
 * @author root
 *
 */
public interface FileNodeSinkManager extends LifeCycle {
	/**
	 * 注册一个文件接受槽
	 * 
	 * @param sink
	 * @throws Exception
	 */
	void registerFileNodeSink(FileNodeSink sink) throws Exception;
}
