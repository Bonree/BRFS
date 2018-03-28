package com.bonree.brfs.common.utils;

/**
 * 模块的生命周期管理接口
 * 
 * @author chen
 *
 */
public interface LifeCycle {
	/**
	 * 开始模块的运行
	 * 
	 * @throws Exception
	 */
	void start() throws Exception;
	
	/**
	 * 结束模块运行
	 * 
	 * @throws Exception
	 */
	void stop() throws Exception;
}
