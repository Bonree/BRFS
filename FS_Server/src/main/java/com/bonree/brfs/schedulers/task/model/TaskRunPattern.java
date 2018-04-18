package com.bonree.brfs.schedulers.task.model;

public class TaskRunPattern {
	private int repeateCount = 1;
	private long sleepTime = 2000;
	public int getRepeateCount() {
		return repeateCount;
	}
	public void setRepeateCount(int repeateCount) {
		this.repeateCount = repeateCount;
	}
	public long getSleepTime() {
		return sleepTime;
	}
	public void setSleepTime(long sleepTime) {
		this.sleepTime = sleepTime;
	}
}
