package com.bonree.brfs.schedulers.task.model;

import java.util.ArrayList;
import java.util.List;

public class AtomTaskResultModel {
	private String sn;
	private String dir;
	private List<String> files = new ArrayList<String>();
	private int operationFileCount = 0;
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
}
