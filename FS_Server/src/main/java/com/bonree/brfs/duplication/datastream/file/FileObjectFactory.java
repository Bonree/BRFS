package com.bonree.brfs.duplication.datastream.file;

import com.bonree.brfs.duplication.storagename.StorageNameNode;

public interface FileObjectFactory {
	FileObject createFile(StorageNameNode storageRegion);
}
