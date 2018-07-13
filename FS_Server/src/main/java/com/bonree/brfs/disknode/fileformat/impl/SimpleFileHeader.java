package com.bonree.brfs.disknode.fileformat.impl;

import com.bonree.brfs.common.write.data.FileEncoder;
import com.bonree.brfs.disknode.fileformat.FileHeader;
import com.google.common.primitives.Bytes;

public class SimpleFileHeader implements FileHeader {
	
	private static final int DEFAULT_HEADER_VERSION = 0;
	private static final int DEFAULT_HEADER_TYPE = 0;

	@Override
	public byte[] getBytes() {
		return Bytes.concat(FileEncoder.start(), FileEncoder.header(DEFAULT_HEADER_VERSION, DEFAULT_HEADER_TYPE));
	}

	@Override
	public int length() {
		return 2;
	}

}
