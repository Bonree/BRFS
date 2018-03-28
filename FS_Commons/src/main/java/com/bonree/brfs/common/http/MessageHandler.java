package com.bonree.brfs.common.http;

/**
 * 
 * 消息处理接口，通过Callback的形式实现异步调用
 * 
 * @author chen
 *
 * @param <T>
 */
public interface MessageHandler<T> {
	void handle(T msg, HandleResultCallback callback);
}
