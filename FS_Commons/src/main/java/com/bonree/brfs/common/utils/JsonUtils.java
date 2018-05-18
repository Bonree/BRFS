package com.bonree.brfs.common.utils;

import com.alibaba.fastjson.JSON;

/**
 * Json与Java对象之间的转换工具类
 * 
 * @author chen
 *
 */
public final class JsonUtils {

	/**
	 * Java对象转换为Json字符串
	 * 
	 * @param obj
	 * @return
	 */
	public static <T> String toJsonString(T obj) {
		return JSON.toJSONString(obj);
	}
	
	/**
	 * 从Json字符串解析Java对象
	 * 
	 * @param jsonString
	 * @param cls
	 * @return
	 */
	public static <T> T toObject(String jsonString, Class<T> cls) {
		return JSON.toJavaObject(JSON.parseObject(jsonString), cls);
	}

	/**
	 * 把Java对象转化为Json形式的字节数组
	 * 
	 * @param obj
	 * @return
	 */
	public static <T> byte[] toJsonBytes(T obj) {
		return JSON.toJSONBytes(obj);
	}
	
	/**
	 * 从Json形式的字节数组中解析Java对象
	 * 
	 * @param jsonBytes
	 * @param cls
	 * @return
	 */
	public static <T> T toObject(byte[] jsonBytes, Class<T> cls) {
		return JSON.parseObject(jsonBytes, cls);
	}
	
	private JsonUtils() {}
}
