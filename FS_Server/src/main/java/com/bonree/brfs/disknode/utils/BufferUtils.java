package com.bonree.brfs.disknode.utils;

import java.nio.MappedByteBuffer;

import sun.misc.Cleaner;
import sun.nio.ch.DirectBuffer;

public final class BufferUtils {
	
	public static void release(MappedByteBuffer buf) {
		if(buf != null && buf.isDirect()) {
			DirectBuffer directBuffer = (DirectBuffer) buf;
			Cleaner cleaner = directBuffer.cleaner();
			if(cleaner != null) {
				cleaner.clean();
			}
		}
	}

	private BufferUtils() {}
}
