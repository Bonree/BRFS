package com.bonree.brfs.client.impl;

import com.bonree.brfs.client.InputItem;

public class SimpleDataItem implements InputItem {
	private byte[] bytes;
	
	public SimpleDataItem(byte[] bytes) {
		this.bytes = bytes;
	}

	@Override
	public byte[] getBytes() {
		return bytes;
	}

}
