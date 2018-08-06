package com.bonree.brfs.schedulers.task.model;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * *****************************************************************************
 * 版权信息：北京博睿宏远数据科技股份有限公司
 * Copyright: Copyright (c) 2007北京博睿宏远数据科技股份有限公司,Inc.All Rights Reserved.
 * 
 * @date 2018年6月5日 下午4:33:08
 * @Author: <a href=mailto:zhucg@bonree.com>朱成岗</a>
 * @Description:任务类型节点 ,存储各个sn的创建
 *****************************************************************************
 */
public class TaskTypeModel {
	private Map<String,Long> snTimes = new ConcurrentHashMap<>();
	private boolean switchFlag = true;

	public Map<String, Long> getSnTimes() {
		return snTimes;
	}
	public void removesnTime(String sn) {
		if(this.snTimes.containsKey(sn)) {
			this.snTimes.remove(sn);
		}
	}
	public void setSnTimes(Map<String, Long> snTimes) {
		this.snTimes = snTimes;
	}
	public void putSnTimes(String key, long value) {
		this.snTimes.put(key, value);
	}
	public void putAllSnTimes(Map<String,Long> snTimes) {
		this.snTimes.putAll(snTimes);
	}
	public boolean isSwitchFlag() {
		return switchFlag;
	}
	public void setSwitchFlag(boolean switchFlag) {
		this.switchFlag = switchFlag;
	}
}
