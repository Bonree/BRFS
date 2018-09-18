package com.bonree.brfs.common.net.tcp.file;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

public class ReadObject {
	@JsonIgnore
	public static final int RAW_PATH = 1;
	@JsonIgnore
	public static final int RAW_OFFSET = 2;
	@JsonIgnore
	public static final int RAW_LENGTH = 4;
	
	@JsonProperty("token")
	private int token;
	@JsonProperty("path")
	private String filePath;
	@JsonProperty("offset")
	private long offset;
	@JsonProperty("length")
	private int length;
	@JsonProperty("raw")
	private int raw;
	
	public int getToken() {
		return token;
	}

	public void setToken(int token) {
		this.token = token;
	}

	public String getFilePath() {
		return filePath;
	}

	public void setFilePath(String filePath) {
		this.filePath = filePath;
	}

	public long getOffset() {
		return offset;
	}

	public void setOffset(long offset) {
		this.offset = offset;
	}

	public int getLength() {
		return length;
	}

	public void setLength(int length) {
		this.length = length;
	}

	public int getRaw() {
		return raw;
	}

	public void setRaw(int raw) {
		this.raw = raw;
	}
	
	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("{filepath=").append(filePath).append(",")
		       .append("offset=").append(offset).append(",")
		       .append("length=").append(length).append("}");
		
		return builder.toString();
	}
}
