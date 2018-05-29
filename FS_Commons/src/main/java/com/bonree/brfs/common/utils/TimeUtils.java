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
		builder.append(new DateTime(last).toString(TIME_FORMAT)).append('_').append(new DateTime(last + interval).toString(TIME_FORMAT));
		return builder.toString();
	}
}
