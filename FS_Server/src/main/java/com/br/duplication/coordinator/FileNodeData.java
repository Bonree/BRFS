package com.br.duplication.coordinator;

import java.io.ByteArrayOutputStream;

import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;

public class FileNodeData {
	private long createTime;
	private String owner;
	private int[] duplicates;
	
	public FileNodeData() {
		
	}

	public FileNodeData(long createTime, String token, int[] duplicates) {
		this.createTime = createTime;
		this.owner = token;
		this.duplicates = duplicates;
	}

	public long getCreateTime() {
		return createTime;
	}
	
	public String getServerToken() {
		return owner;
	}

	public int[] getDuplicates() {
		return duplicates;
	}
}
