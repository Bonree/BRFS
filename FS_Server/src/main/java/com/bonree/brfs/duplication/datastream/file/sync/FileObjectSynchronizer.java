package com.bonree.brfs.duplication.datastream.file.sync;

import com.bonree.brfs.duplication.datastream.file.FileObject;

public interface FileObjectSynchronizer {
    void synchronize(FileObject fileItem, FileObjectSyncCallback callback);
}
