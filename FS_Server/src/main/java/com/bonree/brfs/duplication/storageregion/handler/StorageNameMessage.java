package com.bonree.brfs.duplication.storageregion.handler;

import com.bonree.brfs.common.utils.Attributes;

public class StorageNameMessage {
	//副本数属性名
    public static final String ATTR_REPLICATION = "replication";
    //数据有效期属性名
    public static final String ATTR_TTL = "ttl";
    public static final String ATTR_FILE_CAPACITY = "fileCapacity";
    public static final String ATTR_FILE_PATITION_DURATION = "filePatitionDuration";
    public static final String ATTR_ENABLE = "enable";
	
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
