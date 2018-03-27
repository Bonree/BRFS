package com.bonree.brfs.duplication.storagename;


public class StorageIdBuilder {
	
	public static int createStorageId() {
		return Long.valueOf(System.currentTimeMillis()).intValue();
	}
}
