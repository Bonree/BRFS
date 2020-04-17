package com.bonree.brfs.duplication.datastream.file.sync;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bonree.brfs.common.lifecycle.LifecycleStart;
import com.bonree.brfs.common.lifecycle.LifecycleStop;
import com.bonree.brfs.common.lifecycle.ManageLifecycle;
import com.bonree.brfs.common.process.LifeCycle;
import com.bonree.brfs.common.service.ServiceManager;
import com.bonree.brfs.common.utils.PooledThreadFactory;
import com.bonree.brfs.duplication.datastream.file.FileObject;
import com.bonree.brfs.duplication.filenode.duplicates.DuplicateNode;

@ManageLifecycle
public class DefaultFileObjectSynchronier implements FileObjectSynchronizer, LifeCycle {
	private static final Logger LOG = LoggerFactory.getLogger(DefaultFileObjectSynchronier.class);
	
	private Thread mainThread = new Thread(new FileSyncExecutor(), "file_sync_main");;
	private FileObjectSyncProcessor processor;
	
	private ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(new PooledThreadFactory("file_sync_sched"));
	private long checkMillis;
	
	private ServiceManager serviceManager;
	
	private LinkedBlockingQueue<FileObjectSyncTask> readyTasks = new LinkedBlockingQueue<FileObjectSyncTask>();
	private List<FileObjectSyncTask> delayedTaskList = new ArrayList<FileObjectSyncTask>();
	
	@Inject
	public DefaultFileObjectSynchronier(FileObjectSyncProcessor processor, ServiceManager serviceManager) {
	    this(processor, serviceManager, 10, TimeUnit.SECONDS);
	}
	
	public DefaultFileObjectSynchronier(FileObjectSyncProcessor processor, ServiceManager serviceManager, long checkPeriod, TimeUnit unit) {
		this.processor = processor;
		this.serviceManager = serviceManager;
		this.checkMillis = unit.toMillis(checkPeriod);
	}

	@LifecycleStart
	public void start() throws Exception {
		mainThread.start();
		scheduler.scheduleAtFixedRate(new DelayedTaskChecker(), 0, checkMillis, TimeUnit.MILLISECONDS);
	}

	@LifecycleStop
	public void stop() throws Exception {
		scheduler.shutdown();
		mainThread.interrupt();
	}

	@Override
	public void synchronize(FileObject file, FileObjectSyncCallback callback) {
		try {
			readyTasks.put(new FileObjectSyncTask(file, callback));
		} catch (InterruptedException e) {
			LOG.error("add file sync task error", e);
		}
	}
	
	private class FileSyncExecutor implements Runnable {
		
		@Override
		public void run() {
			while(!mainThread.isInterrupted()) {
				try {
					FileObjectSyncTask task = readyTasks.take();
					
					if(!processor.process(task)) {
						synchronized(delayedTaskList) {
							delayedTaskList.add(task);
						}
					}
				} catch (InterruptedException e) {
					LOG.warn("file sync executor has been interrupted!");
				} catch (Exception e) {
					LOG.error("file sync executor error", e);
				}
			}
		}
		
	}
	
	private class DelayedTaskChecker implements Runnable {

		@Override
		public void run() {
			FileObjectSyncTask[] tasks = null;
			synchronized(delayedTaskList) {
				tasks = new FileObjectSyncTask[delayedTaskList.size()];
				delayedTaskList.toArray(tasks);
				delayedTaskList.clear();
			}
			
			Map<String, Boolean> serviceStates = new HashMap<String, Boolean>();
			for(FileObjectSyncTask task : tasks) {
				try {
					if(task.isExpired()) {
						readyTasks.put(task);
						continue;
					}
					
					boolean canBesync = true;
					for(DuplicateNode node : task.file().node().getDuplicateNodes()) {
						String nodeToken = nodeToken(node);
						Boolean exist = serviceStates.get(nodeToken);
						if(exist == null) {
							exist = serviceManager.getServiceById(node.getGroup(), node.getId()) != null;
							serviceStates.put(nodeToken, exist);
						}
						
						if(!exist) {
							canBesync = false;
							break;
						}
					}
					
					if(canBesync) {
						readyTasks.put(task);
						continue;
					}
					
					synchronized(delayedTaskList) {
						delayedTaskList.add(task);
					}
				} catch (InterruptedException e) {
					LOG.warn("check has been interrupted!");
				}
			}
		}
		
		private String nodeToken(DuplicateNode node) {
			StringBuilder builder = new StringBuilder();
			builder.append(node.getGroup()).append(",").append(node.getId());
			
			return builder.toString();
		}
		
	}
}
