package com.bonree.brfs.common.utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;


/**
 * Json与Java对象之间的转换工具类
 * 
 * @author chen
 *
 */
public final class JsonUtils {
	private static ObjectMapper mapper = new ObjectMapper();

	/**
	 * Java对象转换为Json字符串
	 * 
	 * @param obj
	 * @return
	 * @throws JsonException 
	 */
	public static <T> String toJsonString(T obj) throws JsonException {
		try {
			return mapper.writeValueAsString(obj);
		} catch (JsonProcessingException e) {
			throw new JsonException("parse object ot json string error", e);
		}
	}
	
	/**
	 * 从Json字符串解析Java对象
	 * 
	 * @param jsonString
	 * @param cls
	 * @return
	 */
	public static <T> T toObject(String jsonString, Class<T> cls) throws JsonException {
		try {
			return mapper.readValue(jsonString, cls);
		} catch (Exception e) {
			throw new JsonException("parse json string ot object", e);
		}
	}

	/**
	 * 把Java对象转化为Json形式的字节数组
	 * 
	 * @param obj
	 * @return
	 * @throws JsonException 
	 */
	public static <T> byte[] toJsonBytes(T obj) throws JsonException {
		try {
			return mapper.writeValueAsBytes(obj);
		} catch (JsonProcessingException e) {
			throw new JsonException("parse object ot json bytes error", e);
		}
	}
	
	/**
	 * 从Json形式的字节数组中解析Java对象
	 * 
	 * @param jsonBytes
	 * @param cls
	 * @return
	 * @throws JsonException 
	 */
	public static <T> T toObject(byte[] jsonBytes, Class<T> cls) throws JsonException {
		try {
			return mapper.readValue(jsonBytes, cls);
		} catch (Exception e) {
			throw new JsonException("parse json bytes ot object error", e);
		}
	}
	
	/**
	 * 解析泛型类
	 * 
	 * @param jsonBytes
	 * @param ref
	 * @return
	 * @throws JsonException
	 */
	public static <T> T toObject(byte[] jsonBytes, TypeReference<T> ref) throws JsonException {
		try {
			return mapper.readValue(jsonBytes, ref);
		} catch (Exception e) {
			throw new JsonException("parse json bytes ot object error", e);
		}
	}
	
	public static class JsonException extends Exception {

		public JsonException() {
			super();
		}

		public JsonException(String message, Throwable cause,
				boolean enableSuppression, boolean writableStackTrace) {
			super(message, cause, enableSuppression, writableStackTrace);
		}

		public JsonException(String message, Throwable cause) {
			super(message, cause);
		}

		public JsonException(String message) {
			super(message);
		}

		public JsonException(Throwable cause) {
			super(cause);
		}

		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;
		
	}
	
	private JsonUtils() {}
}
