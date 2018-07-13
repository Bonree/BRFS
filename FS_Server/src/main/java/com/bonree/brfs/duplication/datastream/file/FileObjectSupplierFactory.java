package com.bonree.brfs.duplication.datastream.file;

import com.bonree.brfs.duplication.storagename.StorageNameNode;

public interface FileObjectSupplierFactory {
	FileObjectSupplier create(StorageNameNode storageRegion);
}
