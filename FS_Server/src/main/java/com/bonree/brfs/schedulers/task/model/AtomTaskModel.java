package com.bonree.brfs.schedulers.task.model;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import com.bonree.brfs.common.utils.TimeUtils;
import com.bonree.brfs.duplication.storageregion.StorageRegion;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/******************************************************************************
 * 版权信息：北京博睿宏远数据科技股份有限公司
 * Copyright: Copyright (c) 2007北京博睿宏远数据科技股份有限公司,Inc.All Rights Reserved.
 * 
 * @date 2018年4月11日 下午4:58:56
 * @Author: <a href=mailto:zhucg@bonree.com>朱成岗</a>
 * @Description: 任务原子任务
 *****************************************************************************
 */
@JsonIgnoreProperties(ignoreUnknown = true) 
public class AtomTaskModel {
	private String storageName;
	private String dataStartTime;
	private String dataStopTime;
	private List<String> files = new ArrayList<String>();
	private String taskOperation;
	private long granule;
	private int patitionNum;
	/**
	 * 概述：生成任务信息
	 * @param atomFiles
	 * @param snName
	 * @param taskOperation
	 * @param dir
	 * @param startTime
	 * @param endTime
	 * @return
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	public static AtomTaskModel getInstance(Collection<String> atomFiles, String snName, String taskOperation,int patitionNum, long startTime, long endTime,long granule) {
		AtomTaskModel atom = new AtomTaskModel();
		if(atomFiles != null) {
			atom.addAllFiles(atomFiles);
		}
		atom.setStorageName(snName);
		atom.setTaskOperation(taskOperation);
		atom.setDataStartTime(TimeUtils.formatTimeStamp(startTime, TimeUtils.TIME_MILES_FORMATE));
		atom.setDataStopTime(TimeUtils.formatTimeStamp(endTime, TimeUtils.TIME_MILES_FORMATE));
		atom.setPatitionNum(patitionNum);
		atom.setGranule(granule);
		return atom;
	}
	
	public String getStorageName() {
		return storageName;
	}
	public void setStorageName(String storageName) {
		this.storageName = storageName;
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
	public void addAllFiles(Collection<String> files){
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

	public long getGranule() {
		return granule;
	}

	public void setGranule(long granule) {
		this.granule = granule;
	}

	public int getPatitionNum() {
		return patitionNum;
	}

	public void setPatitionNum(int patitionNum) {
		this.patitionNum = patitionNum;
	}
}
