package com.br.disknode.client;

import java.io.IOException;

import org.apache.http.impl.client.HttpClients;

public class Test {

	public static void main(String[] args) throws IOException {
		DiskNodeClient client = new HttpDiskNodeClient("localhost", 8899);
		
		boolean init = client.initFile("/root/temp/t1", true);
		System.out.println("init--" + init);
		
//		client.writeData("/root/temp/t1", "12345qwerty".getBytes());
		
		client.closeFile("/root/temp/t1");
		
//		byte[] bs = client.readData("/root/temp/t1", 0, 100);
//		System.out.println("read--" + new String(bs));
		
	}

}
