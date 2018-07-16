package com.bonree.brfs.duplication.datastream.dataengine;

import com.bonree.brfs.duplication.storageregion.StorageRegion;

public interface DataEngineFactory {
	DataEngine createDataEngine(StorageRegion storageRegion);
}
