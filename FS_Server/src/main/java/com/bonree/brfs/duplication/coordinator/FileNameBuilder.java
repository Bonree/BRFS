package com.bonree.brfs.duplication.coordinator;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

public class FileNameBuilder {
	private static AtomicInteger id = new AtomicInteger(0);
	
	public static String createFile() {
//		return UUID.randomUUID().toString();
		return "file_" + id.getAndIncrement();
	}
}
