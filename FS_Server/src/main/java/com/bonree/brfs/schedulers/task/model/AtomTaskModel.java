package com.bonree.brfs.schedulers.task.model;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

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
	private String dataStartTime;
	private String dataStopTime;
	private String dirName;
	private ArrayList<String> files = new ArrayList<String>();
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
	public List<String> getFiles() {
		return files;
	}
	public void setFiles(List<String> files) {
		this.files.addAll(files);
	}
	public void setFiles(ArrayList<String> files) {
		this.files = files;
	}
	public void addAllFiles(ArrayList<String> files){
		this.files.addAll(files);
	}
	public void addFile(String file){
		this.files.add(file);
	}
	public String getDataStartTime() {
		return dataStartTime;
	}
	public void setDataStartTime(String dataStartTime) {
		this.dataStartTime = dataStartTime;
	}
	public String getDataStopTime() {
		return dataStopTime;
	}
	public void setDataStopTime(String dataStopTime) {
		this.dataStopTime = dataStopTime;
	}
}
