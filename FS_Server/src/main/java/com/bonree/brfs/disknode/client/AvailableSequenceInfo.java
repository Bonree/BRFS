package com.bonree.brfs.disknode.client;

import java.util.BitSet;

public class AvailableSequenceInfo {
	private String serviceGroup;
	private String serviceId;
	private String filePath;
	private byte[] availableSequence;

	public String getServiceGroup() {
		return serviceGroup;
	}

	public void setServiceGroup(String serviceGroup) {
		this.serviceGroup = serviceGroup;
	}

	public String getServiceId() {
		return serviceId;
	}

	public void setServiceId(String serviceId) {
		this.serviceId = serviceId;
	}
	
	public String getFilePath() {
		return filePath;
	}
	
	public void setFilePath(String filePath) {
		this.filePath = filePath;
	}

	public BitSet getAvailableSequence() {
		return availableSequence == null ? null : BitSet.valueOf(availableSequence);
	}

	public void setAvailableSequence(BitSet availableSequence) {
		this.availableSequence = availableSequence == null ? null : availableSequence.toByteArray();
	}
}
