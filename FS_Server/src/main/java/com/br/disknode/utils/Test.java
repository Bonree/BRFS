package com.br.disknode.utils;


public class Test {

	public static void main(String[] args) throws InterruptedException {
		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				System.out.println("shutdown thread run...");
			}
		});
		
		Thread.sleep(60000);
	}

}
