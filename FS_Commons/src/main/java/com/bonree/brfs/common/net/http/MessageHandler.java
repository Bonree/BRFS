package com.bonree.brfs.common.net.http;

/**
 * 
 * 消息处理接口，通过Callback的形式实现异步调用
 * 
 * @author chen
 *
 * @param <T>
 */
public interface MessageHandler {
	/**
	 * 请求数据数据是否有效
	 * 
	 * @param message
	 * @return 有效返回true；否则返回false
	 */
	boolean isValidRequest(HttpMessage message);
	
	/**
	 * 对请求进行处理
	 * 
	 * @param msg
	 * @param callback
	 */
	void handle(HttpMessage msg, HandleResultCallback callback);
}
