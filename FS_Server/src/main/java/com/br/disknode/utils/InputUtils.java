package com.br.disknode.utils;

import java.io.IOException;
import java.io.InputStream;

public class InputUtils {
	
	public static void readBytes(InputStream input, byte[] des, int offset, int length) throws IOException {
		int read = 0;
		
		read = input.read(des, offset, length);
		System.out.println("########read===" + read);
		while(length > 0 && (read) >= 0) {
			offset += read;
			length -= read;
			
			break;
		}
	}
}
