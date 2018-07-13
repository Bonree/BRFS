package com.bonree.brfs.duplication.datastream.file;

import java.io.Closeable;

public interface FileObjectSupplier extends Closeable {
	FileObject fetch(int size) throws InterruptedException;
	void recycle(FileObject file, boolean needSync);
}
