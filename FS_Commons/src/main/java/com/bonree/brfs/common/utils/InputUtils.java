package com.bonree.brfs.common.utils;

import java.io.IOException;
import java.io.InputStream;

public class InputUtils {
	
	public static void readBytes(InputStream input, byte[] des, int offset, int length) throws IOException {
		int read = 0;
		
		while(length > 0 && (read = input.read(des, offset, length)) >= 0) {
			offset += read;
			length -= read;
		}
	}
}
