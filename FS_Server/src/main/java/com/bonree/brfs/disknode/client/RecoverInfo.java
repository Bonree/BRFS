package com.bonree.brfs.disknode.client;

import java.util.ArrayList;
import java.util.List;

public class RecoverInfo {
	private int maxSeq;
	private List<AvailableSequenceInfo> infoList = new ArrayList<AvailableSequenceInfo>();

	public int getMaxSeq() {
		return maxSeq;
	}

	public void setMaxSeq(int maxSeq) {
		this.maxSeq = maxSeq;
	}

	public List<AvailableSequenceInfo> getInfoList() {
		return infoList;
	}

	public void setInfoList(List<AvailableSequenceInfo> infoList) {
		this.infoList = infoList;
	}
}
