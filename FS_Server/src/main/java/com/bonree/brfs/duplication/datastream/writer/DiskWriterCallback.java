package com.bonree.brfs.duplication.datastream.writer;

import com.bonree.brfs.duplication.FidBuilder;
import com.bonree.brfs.duplication.datastream.dataengine.impl.DataObject;
import com.bonree.brfs.duplication.datastream.file.FileObject;
import com.bonree.brfs.duplication.datastream.writer.DiskWriter.WriteProgressListener;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReferenceArray;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DiskWriterCallback {
    private static final Logger LOG = LoggerFactory.getLogger(DiskWriterCallback.class);

    private final AtomicInteger count;
    private final AtomicReferenceArray<DataOut[]> results;

    private final List<DataObject> dataCallbacks;

    private final WriteProgressListener callback;

    public DiskWriterCallback(int dupCount, List<DataObject> dataCallbacks, WriteProgressListener callback) {
        this.count = new AtomicInteger(dupCount);
        this.results = new AtomicReferenceArray<>(dupCount);
        this.dataCallbacks = dataCallbacks;
        this.callback = callback;
    }

    public void complete(FileObject file, int index, DataOut[] result, boolean isClosed) {
        if (isClosed) {
            handleClosedFile(file);
            return;
        }

        results.set(index, result);

        if (count.decrementAndGet() == 0) {
            handleResults(file);
        }
    }

    private void handleClosedFile(FileObject file) {
        callback.writeCompleted(file, false, true);
    }

    private void handleResults(FileObject file) {
        boolean writeError = false;
        if (results.length() < 1) {
            throw new IllegalStateException(String.format("No results for file[%s]", file.node().getName()));
        }

        DataOut[] maxResult = results.get(0);
        for (int i = 1; i < results.length(); i++) {
            DataOut[] otherDataOut = results.get(i);
            for (int j = 0; j < maxResult.length; j++) {
                if (otherDataOut[j] == null) {
                    LOG.error("Error to write data in[{}, {}]", i, j);
                    writeError = true;
                    continue;
                }

                if (maxResult[j] == null) {
                    LOG.error("Error to write data in[{}, {}]", 0, j);
                    maxResult[j] = otherDataOut[j];
                    writeError = true;
                    continue;
                }

                if (maxResult[j].offset() != otherDataOut[j].offset()
                    || maxResult[j].length() != otherDataOut[j].length()) {
                    // WTF! It's impossiple!
                    LOG.error("Error to handle data writing for file[{}] index[{}, {}], want[{}, {}], but[{}, {}]",
                              file.node().getName(), maxResult.length, j,
                              maxResult[j].offset(), maxResult[j].length(),
                              otherDataOut[j].offset(), otherDataOut[j].length());
                    handleClosedFile(file);
                    return;
                }
            }
        }

        int maxValidIndex = -1;
        for (int i = 0; i < maxResult.length; i++) {
            if (maxResult[i] == null) {
                break;
            }

            maxValidIndex = i;
        }

        LOG.debug("write result with max valid index[{}] in file[{}]", maxValidIndex, file.node().getName());
        file.setLength(
            maxValidIndex < 0 ? file.length() : (maxResult[maxValidIndex].offset() + maxResult[maxValidIndex].length()));
        callback.writeCompleted(file, writeError, false);

        String[] fids = new String[dataCallbacks.size()];
        for (int i = 0; i <= maxValidIndex; i++) {
            long offset = maxResult[i].offset();
            int size = maxResult[i].length();

            fids[i] = FidBuilder.getFid(file.node(), offset, size);
        }

        for (int i = 0; i < fids.length; i++) {
            dataCallbacks.get(i).processComplete(fids[i]);
        }
    }
}
