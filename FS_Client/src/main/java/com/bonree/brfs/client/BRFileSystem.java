package com.bonree.brfs.client;

import java.io.Closeable;
import java.util.Map;

/**
 * 
 * 
 * @author yupeng
 *
 */
public interface BRFileSystem extends Closeable {
	boolean createStorageName(String storageName, Map<String, Object> attrs) throws Exception;
	boolean updateStorageName(String storageName, Map<String, Object> attrs);
	boolean deleteStorageName(String storageName);
	StorageNameStick openStorageName(String storageName);
}
