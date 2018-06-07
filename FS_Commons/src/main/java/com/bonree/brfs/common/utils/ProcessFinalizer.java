package com.bonree.brfs.common.utils;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProcessFinalizer extends Thread {
	private static final Logger LOG = LoggerFactory.getLogger(ProcessFinalizer.class);
	private List<Closeable> closeables = new ArrayList<Closeable>();
	
	public void add(Closeable closeable) {
		closeables.add(closeable);
	}
	
	public void add(LifeCycle lifeCycle) {
		closeables.add(new Closeable() {
			
			@Override
			public void close() throws IOException {
				try {
					lifeCycle.stop();
				} catch (Exception e) {
					LOG.error("stop lifcycle[{}] error", lifeCycle.getClass(), e);
				}
			}
		});
	}

	@Override
	public void run() {
		LOG.info("shutting down service...");
		for(int i = closeables.size() - 1; i >= 0; i--) {
			CloseUtils.closeQuietly(closeables.get(i));
		}
	}
	
}
