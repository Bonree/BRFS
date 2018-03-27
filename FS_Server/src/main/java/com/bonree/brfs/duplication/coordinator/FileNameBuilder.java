package com.bonree.brfs.duplication.coordinator;

import java.util.UUID;

public class FileNameBuilder {
	
	public static String createFile() {
		return UUID.randomUUID().toString();
	}
}
