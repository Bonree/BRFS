package com.bonree.brfs.duplication.storagename.exception;

public class StorageNameRemoveException extends Exception {


    private static final long serialVersionUID = 4770090354473298041L;

    public StorageNameRemoveException(String storageName) {
		super("StorageName[" + storageName + "] is enable!");
	}
	
	public StorageNameRemoveException(int storageNameId) {
		super("StorageNameID[" + storageNameId + "] is enable!");
	}
}
