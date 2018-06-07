package com.bonree.brfs.duplication.datastream.tasks;

public class ResultItem {
	private int sequence;
	private String fid;
	
	public ResultItem() {
		this(0);
	}
	
	public ResultItem(int seq) {
		this(seq, null);
	}
	
	public ResultItem(int seq, String fid) {
		this.sequence = seq;
		this.fid = fid;
	}

	public int getSequence() {
		return sequence;
	}

	public void setSequence(int sequence) {
		this.sequence = sequence;
	}

	public String getFid() {
		return fid;
	}

	public void setFid(String fid) {
		this.fid = fid;
	}
}
