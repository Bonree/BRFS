package com.bonree.brfs.duplication.datastream.connection;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Optional;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

public class Test {

	public static void main(String[] args) throws ExecutionException, InterruptedException {
		LoadingCache<String, Optional<String>> cache = CacheBuilder.newBuilder()
				.expireAfterAccess(1, TimeUnit.SECONDS).build(new CacheLoader<String, Optional<String>>() {

			@Override
			public Optional<String> load(String key) throws Exception {
				System.out.println("build value--" + key);
				if(key.equals("right")) {
					return Optional.of("right");
				} else if(key.equals("wrong")) {
					return Optional.of("nono");
				}
				
				return Optional.absent();
			}
			
		});
		
//		System.out.println("##" + cache.get("right"));
//		System.out.println("##" + cache.get("wrong"));
//		System.out.println("##" + cache.get("right"));
//		
//		cache.refresh("right");
//		
//		System.out.println("##" + cache.get("right", new Callable<String>() {
//
//			@Override
//			public String call() throws Exception {
//				return "another haha";
//			}
//		}));
		
		System.out.println("##" + cache.get("234").get());
	}

}
