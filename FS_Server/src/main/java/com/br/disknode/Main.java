package com.br.disknode;

import java.io.IOException;
import java.util.Random;

public class Main {
	private static byte[] data = "1234567890\n".getBytes();
	private static Random rand = new Random();
	
	private static int back = 0;

	public static void main(String[] args) throws IOException {
		System.out.println("--" + data.length);
		DiskWriter writer = new DiskWriter("/root/temp/T", true, null);
		
		int size = 50000;
		long start = System.currentTimeMillis();
		while(size-- > 0) {
			write(writer, 100);
		}
		writer.close();
		System.out.println("take--" + (System.currentTimeMillis() - start));
		
		System.out.println("back --" + back);
	}
	
	public static void write(DiskWriter writer, int size) {
		try {
			writer.beginWriting();
			//write datas
			while(size-- > 0) {
				writer.write(data);
//				if(size % 100 == 1 && rand.nextInt(10) == 2) {
//					throw new IOException();
//				}
			}
			
			InputResult writeInfo = writer.endWriting();
//			System.out.println("write info=" + writeInfo);
		} catch (Exception e) {
			writer.backWriting();
			back++;
		}
	}

}
