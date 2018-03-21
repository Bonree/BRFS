package com.br.duplication;

import com.br.duplication.data.handler.DataRequestHandler;
import com.br.duplication.data.handler.DeleteDataMessageHandler;
import com.br.duplication.data.handler.ReadDataMessageHandler;
import com.br.duplication.data.handler.WriteDataMessageHandler;
import com.br.duplication.server.netty.NettyHttpContextHandler;
import com.br.duplication.server.netty.NettyHttpServer;
import com.br.duplication.storagename.StorageNameManager;
import com.br.duplication.storagename.StorageNameNode;
import com.br.duplication.storagename.handler.CreateStorageNameMessageHandler;
import com.br.duplication.storagename.handler.DeleteStorageNameMessageHandler;
import com.br.duplication.storagename.handler.OpenStorageNameMessageHandler;
import com.br.duplication.storagename.handler.StorageNameRequestHandler;
import com.br.duplication.storagename.handler.UpdateStorageNameMessageHandler;

public class Main {

	public static void main(String[] args) throws Exception {
		NettyHttpServer server = new NettyHttpServer(8899);
		
		StorageNameRequestHandler storageNameRequestHandler = new StorageNameRequestHandler();
		StorageNameManager storageNameManager = new MockStorageNameManager();
		storageNameManager.start();
		storageNameRequestHandler.addMessageHandler("PUT", new CreateStorageNameMessageHandler(storageNameManager));
		storageNameRequestHandler.addMessageHandler("POST", new UpdateStorageNameMessageHandler(storageNameManager));
		storageNameRequestHandler.addMessageHandler("GET", new OpenStorageNameMessageHandler(storageNameManager));
		storageNameRequestHandler.addMessageHandler("DELETE", new DeleteStorageNameMessageHandler(storageNameManager));
		server.addContextHandler(new NettyHttpContextHandler("/storagename", storageNameRequestHandler));
		
		DataRequestHandler dataRequestHandler = new DataRequestHandler();
		dataRequestHandler.addMessageHandler("POST", new WriteDataMessageHandler());
		dataRequestHandler.addMessageHandler("GET", new ReadDataMessageHandler());
		dataRequestHandler.addMessageHandler("DELETE", new DeleteDataMessageHandler());
		server.addContextHandler(new NettyHttpContextHandler("/data", dataRequestHandler));
		
		server.start();
	}
	
	private static class MockStorageNameManager implements StorageNameManager {

		@Override
		public void start() throws Exception {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void stop() throws Exception {
			// TODO Auto-generated method stub
			
		}

		@Override
		public boolean exists(String storageName) {
			// TODO Auto-generated method stub
			return false;
		}

		@Override
		public StorageNameNode createStorageName(String storageName,
				int replicas, int ttl) {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public boolean updateStorageName(String storageName, int ttl) {
			// TODO Auto-generated method stub
			return false;
		}

		@Override
		public boolean removeStorageName(int storageId) {
			// TODO Auto-generated method stub
			return false;
		}

		@Override
		public boolean removeStorageName(String storageName) {
			// TODO Auto-generated method stub
			return false;
		}

		@Override
		public StorageNameNode findStorageName(String storageName) {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public StorageNameNode findStorageName(int id) {
			// TODO Auto-generated method stub
			return null;
		}
		
	}
}
