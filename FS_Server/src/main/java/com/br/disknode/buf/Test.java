package com.br.disknode.buf;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

import com.br.disknode.InputEvent;
import com.br.disknode.utils.PooledThreadFactory;

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
		
		ExecutorService exe = new ThreadPoolExecutor(5,
				5,
                0L,
                TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<Runnable>(5),
                new PooledThreadFactory("write_worker"));
		
		for(int i = 0; i < 5; i++) {
			exe.submit(() -> System.out.println("worker_"));
		}
	}

}
