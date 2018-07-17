package com.bonree.brfs.schedulers.task.model;

import java.util.ArrayList;
import java.util.List;

public class AtomTaskResultModel {
	private String sn;
	private String dir;
	private String dataStartTime;
	private String dataStopTime;
	private boolean isSuccess = true;
	private long granule;
	
	private List<String> files = new ArrayList<String>();
	private int operationFileCount = 0;
	private String message;
	public String getSn() {
		return sn;
	}
	public void setSn(String sn) {
		this.sn = sn;
	}
	public String getDir() {
		return dir;
	}
	public void setDir(String dir) {
		this.dir = dir;
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
	public long getGranule() {
		return granule;
	}
	public void setGranule(long granule) {
		this.granule = granule;
	}
	
}
