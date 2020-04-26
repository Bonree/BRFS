package com.bonree.brfs.disknode.data.write.worker;

import java.util.List;

/**
 * WriteWorker
 *
 * @author chen
 */
public interface WriteWorkerSelector {
    WriteWorker select(List<WriteWorker> workers);
}
