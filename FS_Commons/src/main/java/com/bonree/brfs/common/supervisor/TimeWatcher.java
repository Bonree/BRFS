package com.bonree.brfs.common.supervisor;

public class TimeWatcher {
	private final long startTime;
	
	public TimeWatcher() {
		this.startTime = System.currentTimeMillis();
	}
	
	public int getElapsedTime() {
		return (int) (System.currentTimeMillis() - startTime);
	}
}
