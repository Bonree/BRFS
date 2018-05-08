package com.bonree.brfs.duplication;

import com.bonree.brfs.duplication.coordinator.FileNode;

public class FidBuilder {
	
	public static String getFid(FileNode node, long offset, int size) {
		StringBuilder builder = new StringBuilder();
		builder.append(node.getStorageName()).append("-").append(node.getName()).append("-")
		       .append(offset).append("-").append(size);
		
		return builder.toString();
	}
}
