package com.br.duplication.coordinator;

import java.util.concurrent.LinkedBlockingQueue;

import com.br.duplication.service.Service;
import com.br.duplication.utils.LifeCycle;

public class FileNodeAssigner implements LifeCycle {
	private LinkedBlockingQueue<Service> downServices = new LinkedBlockingQueue<Service>();
	
	@Override
	public void start() throws Exception {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void stop() throws Exception {
		// TODO Auto-generated method stub
		
	}
	
	public void put(Service service) {
		try {
			downServices.put(service);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
	
	public void run() {
		while(true) {
			try {
				Service service = downServices.take();
				
				if(service == null) {
					continue;
				}
				
				//TODO
				//scan the files of down service and reassign them to other services
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
}
