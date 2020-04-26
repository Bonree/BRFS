package com.bonree.brfs.duplication.datastream.file.sync;

import com.bonree.brfs.common.utils.TimeUtils;
import com.bonree.brfs.duplication.datastream.file.FileObject;

public class FileObjectSyncTask {
    private final FileObject file;
    private final FileObjectSyncCallback callback;

    public FileObjectSyncTask(FileObject file, FileObjectSyncCallback callback) {
        this.file = file;
        this.callback = callback;
    }

    public FileObject file() {
        return file;
    }

    public FileObjectSyncCallback callback() {
        return callback;
    }

    public boolean isExpired() {
        return file.node().getCreateTime() < TimeUtils.prevTimeStamp(System.currentTimeMillis(),
                                                                     file.node().getTimeDurationMillis());
    }
}
