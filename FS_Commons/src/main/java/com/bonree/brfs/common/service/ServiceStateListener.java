package com.bonree.brfs.common.service;

/**
 * 服务状态监听接口
 * 
 * @author chen
 *
 */
public interface ServiceStateListener {
	/**
	 * 服务添加事件
	 * 
	 * @param service 新增的服务
	 */
	void serviceAdded(Service service);
	
	/**
	 * 服务移除事件
	 * 
	 * @param service 移除的服务
	 */
	void serviceRemoved(Service service);
}
