package com.br.disknode.utils;

import io.netty.handler.codec.http.HttpMethod;

import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URL;
import java.net.URLDecoder;
import java.util.HashMap;
import java.util.Map;


public class Test {

	public static void main(String[] args) throws Exception {
//		Runtime.getRuntime().addShutdownHook(new Thread() {
//			@Override
//			public void run() {
//				System.out.println("shutdown thread run...");
//			}
//		});
//		
//		Thread.sleep(60000);
		
		Map<HttpMethod, String> map = new HashMap<HttpMethod, String>();
		map.put(HttpMethod.valueOf("CLOSE"), "1234");
		
		System.out.println(map.get(HttpMethod.valueOf("CLOSE")));
		
		System.out.println("--" + URLDecoder.decode("/mem/data/brfs/t3", "UTF-8"));
	}

}
