package com.bonree.brfs.client;

import java.util.Map;

public interface BRFileSystem {
	boolean createStorageName(String storageName, Map<String, Object> attrs);
	boolean updateStorageName(String storageName, Map<String, Object> attrs);
	boolean deleteStorageName(String storageName);
	StorageNameStick openStorageName(String storageName, boolean createIfNonexistent);
}
