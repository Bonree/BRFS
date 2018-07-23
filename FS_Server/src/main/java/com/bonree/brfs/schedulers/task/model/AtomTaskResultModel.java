package com.bonree.brfs.schedulers.task.model;

import java.util.ArrayList;
import java.util.List;

import com.bonree.brfs.common.utils.TimeUtils;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
@JsonIgnoreProperties(ignoreUnknown = true) 
public class AtomTaskResultModel {
	private String sn;
	private String dataStartTime;
	private String dataStopTime;
	private int partNum;
	private boolean isSuccess = true;
	
	private List<String> files = new ArrayList<String>();
	private int operationFileCount = 0;
	private String message;
	
	public static AtomTaskResultModel getInstance(List<String> files, String snName,long startTime,long endTime,String message,int partNum) {
		AtomTaskResultModel atom = new AtomTaskResultModel();
		atom.setDataStartTime(TimeUtils.formatTimeStamp(startTime, TimeUtils.TIME_MILES_FORMATE));
		atom.setDataStopTime(TimeUtils.formatTimeStamp(endTime, TimeUtils.TIME_MILES_FORMATE));
		atom.setMessage(message);
		atom.setSn(snName);
		atom.setPartNum(partNum);
		atom.setSuccess(true);
		if(files!=null && !files.isEmpty()) {
			atom.setFiles(files);
		}
		return atom;
	}
	public String getSn() {
		return sn;
	}
	public void setSn(String sn) {
		this.sn = sn;
	}
	public List<String> getFiles() {
		return files;
	}
	public void setFiles(List<String> files) {
		this.files = files;
	}
	public int getOperationFileCount() {
		return operationFileCount;
	}
	public void setOperationFileCount(int operationFileCount) {
		this.operationFileCount = operationFileCount;
	}
	public void addAll(List<String> files){
		this.files.addAll(files);
	}
	public void add(String file){
		this.files.add(file);
	}
	public boolean isSuccess() {
		return isSuccess;
	}
	public void setSuccess(boolean isSuccess) {
		this.isSuccess = isSuccess;
	}
	public String getMessage() {
		return message;
	}
	public void setMessage(String message) {
		this.message = message;
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
	public int getPartNum() {
		return partNum;
	}
	public void setPartNum(int partNum) {
		this.partNum = partNum;
	}
}
