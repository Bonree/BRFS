package com.br.disknode;

/**
 * WriteWorker
 * 
 * @author chen
 *
 */
public interface WriteWorkerSelector {
	WriteWorker select(WriteWorker[] workers);
}
