package com.bonree.brfs.duplication.datastream.file;

public class FileLoungeCleaner implements Runnable {
	private FileLounge fileLounge;
	
	public FileLoungeCleaner(FileLounge fileLounge) {
		this.fileLounge = fileLounge;
	}

	@Override
	public void run() {
		fileLounge.clean();
	}

}
