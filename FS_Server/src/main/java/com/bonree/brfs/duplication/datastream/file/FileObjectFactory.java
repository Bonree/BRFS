package com.bonree.brfs.duplication.datastream.file;

import com.bonree.brfs.duplication.storageregion.StorageRegion;

public interface FileObjectFactory {
	FileObject createFile(StorageRegion storageRegion);
}
