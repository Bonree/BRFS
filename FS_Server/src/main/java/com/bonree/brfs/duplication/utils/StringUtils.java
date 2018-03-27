package com.bonree.brfs.duplication.utils;

import java.io.UnsupportedEncodingException;

public final class StringUtils {

	public static byte[] toUtf8Bytes(String s) {
		try {
			return s.getBytes("UTF-8");
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		}
		
		return new byte[0];
	}
	
	private StringUtils() {}
}
