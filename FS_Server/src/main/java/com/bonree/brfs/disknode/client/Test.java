package com.bonree.brfs.disknode.client;

import java.io.IOException;
import java.util.concurrent.Executors;

import org.apache.http.impl.client.HttpClients;

public class Test {

	public static void main(String[] args) throws IOException, InterruptedException {
		DiskNodeClient client = new HttpDiskNodeClient("localhost", 8899);
		
		boolean init1 = client.initFile("/root/temp/t1", true);
		System.out.println("init1--" + init1);
		
//		Thread.sleep(2000);
		
		boolean init2 = client.initFile("/root/temp/t2", true);
		System.out.println("init2--" + init2);
		
		client.writeData("/root/temp/t1", "12345qwerty".getBytes());
		
		client.closeFile("/root/temp/t1");
		
		byte[] bs = client.readData("/root/temp/t1", 0, 100);
		System.out.println("read--" + new String(bs));
		
	}

}
