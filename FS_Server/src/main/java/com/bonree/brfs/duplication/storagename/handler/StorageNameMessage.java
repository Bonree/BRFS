package com.bonree.brfs.duplication.storagename.handler;

import com.bonree.brfs.common.utils.Attributes;

public class StorageNameMessage {
	private String name;
	private Attributes attrs = new Attributes();

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}
	
	public void addAttribute(String name, Object value) {
		attrs.putObject(name, value);
	}
	
	public Attributes getAttributes() {
		return attrs;
	}
}
