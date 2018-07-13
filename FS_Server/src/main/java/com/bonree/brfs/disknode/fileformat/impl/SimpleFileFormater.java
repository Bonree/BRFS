package com.bonree.brfs.disknode.fileformat.impl;

import com.bonree.brfs.disknode.fileformat.FileFormater;
import com.bonree.brfs.disknode.fileformat.FileHeader;
import com.bonree.brfs.disknode.fileformat.FileTailer;
import com.google.common.base.Preconditions;

public class SimpleFileFormater implements FileFormater {
	private final long capacity;
	
	public SimpleFileFormater(long capacity) {
		Preconditions.checkArgument(capacity >= 0);
		this.capacity = capacity;
	}

	@Override
	public FileHeader fileHeader() {
		return new SimpleFileHeader();
	}

	@Override
	public FileTailer fileTailer() {
		return new SimpleFileTailer();
	}

	@Override
	public long maxBodyLength() {
		return capacity - fileHeader().length() - fileTailer().length();
	}

	@Override
	public long relativeOffset(long offset) {
		return offset - fileHeader().length();
	}

	@Override
	public long absoluteOffset(long offset) {
		return offset + fileHeader().length();
	}

}
