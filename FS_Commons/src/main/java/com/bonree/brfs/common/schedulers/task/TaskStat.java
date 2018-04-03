package com.bonree.brfs.common.schedulers.task;

public enum TaskStat {
	UNKONW(0),
	INIT(1),
	RUN(2),
	FINISH(3),
	EXCEPTION(4),
	PAUSE(5);
	private int stat;
	TaskStat(int stat){
		this.stat = stat;
	}
	public int code(){
		return this.stat;
	}
	public static TaskStat valueOf(int stat){
		if(stat == 1){
			return INIT;
		} else if(stat == 2){
			return RUN;
		} else if(stat == 3){
			return FINISH;
		} else if(stat == 4){
			return EXCEPTION;
		} else if(stat == 5){
			return PAUSE;
		} else {
			return UNKONW;
		}
	}
}
