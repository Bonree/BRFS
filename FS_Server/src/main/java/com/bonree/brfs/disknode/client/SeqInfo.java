package com.bonree.brfs.disknode.client;

import java.util.BitSet;

public class SeqInfo {
	private String host;
	private int port;
	private byte[] lack;

	public String getHost() {
		return host;
	}

	public void setHost(String host) {
		this.host = host;
	}

	public int getPort() {
		return port;
	}

	public void setPort(int port) {
		this.port = port;
	}

	public BitSet getIntArray() {
		return lack == null ? null : BitSet.valueOf(lack);
	}

	public void setIntArray(BitSet bitSet) {
		this.lack = bitSet == null ? null : bitSet.toByteArray();
	}
}
