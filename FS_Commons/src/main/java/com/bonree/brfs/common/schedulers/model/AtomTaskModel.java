package com.bonree.brfs.common.schedulers.model;
/******************************************************************************
 * 版权信息：北京博睿宏远数据科技股份有限公司
 * Copyright: Copyright (c) 2007北京博睿宏远数据科技股份有限公司,Inc.All Rights Reserved.
 * 
 * @date 2018年4月11日 下午4:58:56
 * @Author: <a href=mailto:zhucg@bonree.com>朱成岗</a>
 * @Description: 任务原子任务
 *****************************************************************************
 */
public class AtomTaskModel {
	private String storageName;
	private String dirName;
	private String taskOperation;
	public String getStorageName() {
		return storageName;
	}
	public void setStorageName(String storageName) {
		this.storageName = storageName;
	}
	public String getDirName() {
		return dirName;
	}
	public void setDirName(String dirName) {
		this.dirName = dirName;
	}
	public String getTaskOperation() {
		return taskOperation;
	}
	public void setTaskOperation(String taskOperation) {
		this.taskOperation = taskOperation;
	}
}
