package com.br.disknode.buf;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.concurrent.ArrayBlockingQueue;

import com.br.disknode.WriteItem;

public class Test {

	public static void main(String[] args) throws IOException, InterruptedException {
//		RandomAccessFile file = new RandomAccessFile("/root/temp/T", "rw");
//		WriteBuffer buf = new SimpleWriteBuffer(file, 20);
//		
//		System.out.println("size==" + buf.size());
//		System.out.println("capacity==" + buf.capacity());
//		int n = 0;
//		while((n = buf.write("123".getBytes())) != 0) {
//			System.out.println("write--" + n);
//		}
//		System.out.println("size==" + buf.size());
//		System.out.println("capacity==" + buf.capacity());
//		buf.flush();
//		
//		file.close();
//		
//		ByteBuffer b = ByteBuffer.allocate(100);
//		b.put("123".getBytes());
//		System.out.println("d-" + b.position());
//		System.out.println("len=" + b.array().length);
		
		ArrayBlockingQueue<Integer> itemQueue = new ArrayBlockingQueue<Integer>(100);
		
		for(int i = 0; i < 10; i++) {
			itemQueue.put(100);
		}
		
		System.out.println("queue size==" + itemQueue.size());
	}

}
