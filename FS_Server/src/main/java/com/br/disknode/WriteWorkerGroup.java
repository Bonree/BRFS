package com.br.disknode;

public class WriteWorkerGroup {
	private WriteWorker[] workers;
	
	public WriteWorkerGroup(int capacity) {
		this.workers = new WriteWorker[capacity];
	}
	
	public int capacity() {
		return workers.length;
	}
	
	public WriteWorker[] workerArray() {
		return workers;
	}
	
	public void fill(Initializer initializer) {
		for(int i = 0; i < workers.length; i++) {
			workers[i] = initializer.init();
		}
	}
	
	public void forEach(Visitor visitor) {
		for(WriteWorker worker : workers) {
			try {
				visitor.visit(worker);
			} catch(Exception e) {}
		}
	}
	
	public static interface Initializer {
		WriteWorker init();
	}
	
	public static interface Visitor {
		void visit(WriteWorker worker);
	}
}
