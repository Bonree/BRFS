package com.bonree.brfs.common.utils;

import org.joda.time.DateTime;

public final class TimeUtils {
	private static final String TIME_FORMAT = "yyyy-MM-dd'T'HH-mm-ss";
	
	/**
	 * 获取指定时间所在的时间区间字符串
	 * 
	 * @param now
	 * @param interval
	 * @return
	 */
	public static String timeInterval(long now, long interval) {
		StringBuilder builder = new StringBuilder();
		long last = (now - now % interval);
		builder.append(formatTimeStamp(last)).append('_').append(formatTimeStamp(last + interval));
		return builder.toString();
	}
	
	/**
	 * 格式化时间戳为时间字符串
	 * 
	 * 比如 2018-03-12T13-34-45.234
	 * 
	 * @param time
	 * @return
	 */
	public static String formatTimeStamp(long time) {
		return new DateTime(time).toString(TIME_FORMAT);
	}
}
