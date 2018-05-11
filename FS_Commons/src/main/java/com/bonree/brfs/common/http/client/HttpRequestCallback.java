package com.bonree.brfs.common.http.client;

/**
 * 监听Http请求的响应状态
 * 
 * @author yupeng
 *
 */
public interface HttpRequestCallback {
	/**
	 * 响应正常
	 * 
	 * @param bytes 响应携带的数据
	 */
	void responseOk(byte[] bytes);
	
	/**
	 * 响应错误
	 * 
	 * @param code
	 * @param reason
	 */
	void responseError(int code, String reason);
	
	/**
	 * 请求发送失败, 主要指没有收到响应的情况
	 */
	void requestFailed(Throwable e);
}
