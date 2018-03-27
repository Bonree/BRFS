package com.bonree.brfs.duplication.data;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import com.bonree.brfs.disknode.client.DiskNodeClient;
import com.bonree.brfs.disknode.client.HttpDiskNodeClient;
import com.bonree.brfs.disknode.client.WriteResult;
import com.bonree.brfs.duplication.coordinator.FileNode;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;

public class DataEmitter {
	
	private ListeningExecutorService execs = MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(5));
	
	private Map<Integer, DiskNodeClient> clientCaches = new HashMap<Integer, DiskNodeClient>();
	
	public void emit(byte[] data, FileNode fileNode, WriteCallback callback) {
		int[] duplicates = fileNode.getDuplicates();
		
		DiskNodeClient[] clients = new DiskNodeClient[duplicates.length];
		for(int i = 0; i < duplicates.length; i++) {
			if(!clientCaches.containsKey(duplicates[i])) {
				DiskNodeClient client = new HttpDiskNodeClient("localhost", 8080);
				clientCaches.put(duplicates[i], client);
			}
			
			clients[i] = clientCaches.get(duplicates[i]);
		}
		
		for(DiskNodeClient client : clients) {
			ResultGather gather = new ResultGather(clients.length, callback);
			ListenableFuture<WriteResult> f = execs.submit(new WriteTask(client, fileNode, data));
			Futures.addCallback(f, gather, execs);
		}
	}
	
	public static interface WriteCallback {
		void success(WriteResult result);
		void recover();
	}
	
	private static class WriteTask implements Callable<WriteResult> {
		private DiskNodeClient client;
		private FileNode fileNode;
		private byte[] data;
		
		public WriteTask(DiskNodeClient client, FileNode fileNode, byte[] data) {
			this.client = client;
			this.fileNode = fileNode;
			this.data = data;
		}

		@Override
		public WriteResult call() throws Exception {
			return client.writeData(fileNode.getName(), data);
		}
		
	}
	
	private static class ResultGather implements FutureCallback<WriteResult> {
		
		private AtomicInteger count;
		
		private WriteResult[] results;
		
		private WriteCallback callback;
		
		private ResultGather(int count, WriteCallback callback) {
			this.count = new AtomicInteger(count);
			this.results = new WriteResult[count];
			this.callback = callback;
		}
		
		private void notifyIfCountdown() {
			if(count.get() == 0) {
				boolean needRecovery = false;
				WriteResult result = null;
				for(int i = 0; i < results.length - 1; i++) {
					if(results[i] == null) {
						needRecovery = true;
					} else {
						result = results[i];
					}
				}
				
				if(result != null) {
					callback.success(result);
				}
				
				if(needRecovery) {
					callback.recover();
				}
			}
		}

		@Override
		public void onSuccess(WriteResult result) {
			results[count.decrementAndGet()] = result;
			
			notifyIfCountdown();
		}

		@Override
		public void onFailure(Throwable t) {
			count.decrementAndGet();
			
			notifyIfCountdown();
		}
		
	}
}
