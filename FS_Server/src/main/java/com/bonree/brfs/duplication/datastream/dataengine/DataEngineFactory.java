package com.bonree.brfs.duplication.datastream.dataengine;

import com.bonree.brfs.duplication.storagename.StorageNameNode;

public interface DataEngineFactory {
	DataEngine createDataEngine(StorageNameNode storageRegion);
}
