package com.bonree.brfs.duplication.storagename.exception;

public class StorageNameExistException extends Exception {

	private static final long serialVersionUID = 4815370576618682366L;

    public StorageNameExistException(String storageName) {
		super("StorageName[" + storageName + "] is already existed!");
	}
	
	public StorageNameExistException(int storageNameId) {
		super("StorageNameID[" + storageNameId + "] is already existed!");
	}
}
