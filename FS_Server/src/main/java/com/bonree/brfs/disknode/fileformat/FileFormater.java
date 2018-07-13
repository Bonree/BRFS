package com.bonree.brfs.disknode.fileformat;

public interface FileFormater {
	FileHeader fileHeader();
	FileTailer fileTailer();
	long maxBodyLength();
	long relativeOffset(long offset);
	long absoluteOffset(long offset);
}
