package com.bonree.brfs.common.utils;

import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

public final class TimeUtils {
	private static final String TIME_FORMAT = "yyyy-MM-dd'T'HH-mm-ss";
	public static final String TIME_MILES_FORMATE = "yyyy-MM-dd HH:mm:ss.SSS";
	
	/**
	 * 获取指定时间所在的时间区间字符串
	 * 
	 * @param now
	 * @param interval
	 * @return
	 */
	public static String timeInterval(long now, long interval) {
		StringBuilder builder = new StringBuilder();
		builder.append(formatTimeStamp(prevTimeStamp(now, interval)))
		.append('_')
		.append(formatTimeStamp(nextTimeStamp(now, interval)));
		return builder.toString();
	}
	
	public static long prevTimeStamp(long time, long interval) {
		return (time - time % interval);
	}
	
	public static long nextTimeStamp(long time, long interval) {
		return prevTimeStamp(time, interval) + interval;
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
	
	public static String formatTimeStamp(long time, String timeFormate) {
		return new DateTime(time).toString(timeFormate);
	}
	/**
	 * 概述：时间字符串转换为时间戳
	 * @param timeStr
	 * @return
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	public static long getMiles(String timeStr,String timeFormate){
		long time = 0;
		try {
			DateTimeFormatter format = DateTimeFormat.forPattern(timeFormate);
			time = new DateTime().parse(timeStr, format).getMillis();
		}
		catch (Exception e) {
			e.printStackTrace();
		}
		return time;
	}
	
	public static long getMiles(String timeStr){
		return getMiles(timeStr, TIME_FORMAT);
	}
}
