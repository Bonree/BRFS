package com.bonree.brfs.disknode;

/**
 * WriteWorker
 * 
 * @author chen
 *
 */
public interface WriteWorkerSelector {
	WriteWorker select(WriteWorker[] workers);
}
