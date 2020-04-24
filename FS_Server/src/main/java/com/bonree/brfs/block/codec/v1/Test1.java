package com.bonree.brfs.block.codec.v1;


public class Test1 {
	private static final int MASK_BIT = 7;
	private static final byte MASK = (byte) ((1 << MASK_BIT) - 1);

	public static void main(String[] args) {
		int n = 1024 * 3 + 123;
		
		byte[] bs = encodeInt(n);
		System.out.println(n + " : " + bs.length);
		
		System.out.println("->" + decodeInt(bs));
	}
	
	private static byte[] encodeInt(int n) {
		byte[] bs = new byte[count(n)];
		
		for(int i = bs.length - 1; i >= 0; i--) {
			bs[i] = (byte) (n & MASK);
			n = n >>> MASK_BIT;
		}
		
		bs[0] |= 1 << MASK_BIT;
		
		return bs;
	}
	
	private static int decodeInt(byte[] bs) {
		int n = 0;
		bs[0] &= MASK;
		
		for(int i = 0; i < bs.length; i++) {
			n = n << MASK_BIT;
			n |= bs[i];
		}
		
		return n;
	}
	
	public static int count(int n) {
		int c = 1;
		while((n = n >>> MASK_BIT) != 0) {
			c++;
		}
		
		return c;
	}
	
	public static int clp(int x) {
		x = x - 1;
		x = x | (x >> 1);
		x = x | (x >> 2);
		x = x | (x >> 4);
		x = x | (x >> 8);
		x = x | (x >> 16);
		
		return x + 1;
	}
}
