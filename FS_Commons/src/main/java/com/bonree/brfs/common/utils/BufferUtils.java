package com.bonree.brfs.common.utils;

import java.nio.ByteBuffer;

import sun.misc.Cleaner;
import sun.nio.ch.DirectBuffer;

@SuppressWarnings("restriction")
public final class BufferUtils {
	
	/**
	 * 释放缓存对象
	 * 
	 * @param buf
	 */
	public static void release(ByteBuffer buf) {
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
