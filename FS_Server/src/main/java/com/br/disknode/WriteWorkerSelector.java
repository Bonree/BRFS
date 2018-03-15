package com.br.disknode;

import java.util.List;

public interface WriteWorkerSelector {
	WriteWorker select(List<WriteWorker> workers);
}
