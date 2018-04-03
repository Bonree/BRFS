package com.bonree.brfs.common.schedulers.task;

public enum TaskType {
	UNDEFINE(0),
	SYSTEM_DELETE(1),
	SYSTEM_MERGER(2),
	SYSTEM_CHECK(3),
	USER_DELETE(4);
	private int index = 0;
	TaskType(int index){
		this.index = index;
	}
	public static TaskType valueOf(int index){
		if(1 == index){
			return SYSTEM_DELETE;
		}else if(2 == index){
			return SYSTEM_MERGER;
		}else if(3 == index){
			return SYSTEM_CHECK;
		}else if(4 == index){
			return USER_DELETE;
		}else {
			return UNDEFINE;
		}
	}
	public int code(){
		return this.index;
	}
}
