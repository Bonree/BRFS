package com.bonree.brfs.common.timer;

import java.util.concurrent.TimeUnit;

public class TimeCounter {
	private String name;
	private long startTime;
	private TimeUnit unit;
	
	public TimeCounter(String name, TimeUnit unit) {
		this.name = name;
		this.unit = unit;
	}
	
	public void begin() {
		startTime = System.nanoTime();
	}
	
	private long elapsedTime() {
		return TimeUnit.NANOSECONDS.convert(System.nanoTime() - startTime, unit);
	}
	
	public String report(int index) {
		StringBuilder builder = new StringBuilder();
		builder.append("TimeCounter[").append(name).append("] at index[").append(index).append("] take time-->[")
		.append(elapsedTime()).append("]");
		
		return builder.toString();
	}
}
