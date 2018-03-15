package com.br.disknode.watch;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import com.br.disknode.utils.PooledThreadFactory;
import com.google.common.collect.ArrayListMultimap;

public class WatchMarket {
	
	private ArrayListMultimap<String, Watcher<? extends Object>> watchers = ArrayListMultimap.create();
	
	private ArrayListMultimap<String, WatchListener> listeners = ArrayListMultimap.create();
	
	private static final int DEFAULT_WATCH_PERIOD_SECONDS = 10;
	private ScheduledExecutorService singleThread = Executors.newSingleThreadScheduledExecutor(new PooledThreadFactory("watch_market"));
	private ScheduledFuture<?> manageFuture;
	
	private static WatchMarket instance = new WatchMarket();
	private WatchMarket() {}
	
	public static WatchMarket get() {
		return instance;
	}
	
	public <T> void addWatcher(String name, Watcher<T> watcher) {
		watchers.put(name, watcher);
	}
	
	public <T> void removeWatcher(String name, Watcher<T> watcher) {
		watchers.remove(name, watcher);
	}
	
	public void addListener(String name, WatchListener listener) {
		listeners.put(name, listener);
	}
	
	public void start() {
		manageFuture = singleThread.scheduleAtFixedRate(new Management(), 0, DEFAULT_WATCH_PERIOD_SECONDS, TimeUnit.SECONDS);
	}
	
	public void stop() {
		manageFuture.cancel(true);
		singleThread.shutdown();
	}
	
	private class Management implements Runnable {

		@Override
		public void run() {
			for(String name : listeners.keySet()) {
				List<WatchListener> listenerList = listeners.get(name);
				
				if(watchers.containsKey(name)) {
					List<Watcher<?>> watcherList = watchers.get(name);
					List<Object> metrics = new ArrayList<Object>();
					for(Watcher<?> watcher : watcherList) {
						try {
							Object metric = watcher.watch();
							if(metric != null) {
								metrics.add(metric);
							}
						} catch (Exception e) {
						}
					}
					
					for(WatchListener listener : listenerList) {
						try {
							listener.watchHappened(metrics);
						} catch (Exception e) {
						}
					}
				}
			}
		}
		
	}
}
