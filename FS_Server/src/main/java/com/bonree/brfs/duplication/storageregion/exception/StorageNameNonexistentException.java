package com.bonree.brfs.duplication.storageregion.exception;

public class StorageNameNonexistentException extends Exception {
	/**
	 * 
	 */
	private static final long serialVersionUID = 7325191699140082804L;

	public StorageNameNonexistentException(String storageName) {
		super("StorageName[" + storageName + "] is not existed!");
	}
	
	public StorageNameNonexistentException(int storageNameId) {
		super("StorageNameID[" + storageNameId + "] is not existed!");
	}
}
