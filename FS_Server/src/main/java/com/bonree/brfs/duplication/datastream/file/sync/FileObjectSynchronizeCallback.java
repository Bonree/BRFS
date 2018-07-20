package com.bonree.brfs.duplication.datastream.file.sync;

import com.bonree.brfs.duplication.datastream.file.FileObject;

public interface FileObjectSynchronizeCallback {
	void complete(FileObject file, long fileLength);
	void timeout(FileObject file);
}
