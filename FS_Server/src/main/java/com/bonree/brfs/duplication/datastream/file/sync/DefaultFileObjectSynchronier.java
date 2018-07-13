package com.bonree.brfs.duplication.datastream.file.sync;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bonree.brfs.common.process.LifeCycle;
import com.bonree.brfs.common.service.Service;
import com.bonree.brfs.common.service.ServiceManager;
import com.bonree.brfs.common.service.ServiceStateListener;
import com.bonree.brfs.common.timer.TimeExchangeEventEmitter;
import com.bonree.brfs.common.timer.TimeExchangeListener;
import com.bonree.brfs.common.utils.PooledThreadFactory;
import com.bonree.brfs.configuration.Configs;
import com.bonree.brfs.configuration.units.CommonConfigs;
import com.bonree.brfs.disknode.client.DiskNodeClient;
import com.bonree.brfs.duplication.datastream.FilePathMaker;
import com.bonree.brfs.duplication.datastream.connection.DiskNodeConnection;
import com.bonree.brfs.duplication.datastream.connection.DiskNodeConnectionPool;
import com.bonree.brfs.duplication.datastream.file.FileObject;
import com.bonree.brfs.duplication.filenode.FileNode;
import com.bonree.brfs.duplication.filenode.duplicates.DuplicateNode;

public class DefaultFileObjectSynchronier implements FileObjectSynchronizer, TimeExchangeListener, LifeCycle {
	private static final Logger LOG = LoggerFactory.getLogger(DefaultFileObjectSynchronier.class);
	
	private static final int DEFAULT_THREAD_NUM = 1;
	private ExecutorService threadPool;
	
	private DiskNodeConnectionPool connectionPool;
	
	private FilePathMaker pathMaker;
	
	private ServiceManager serviceManager;
	private ServiceStateListener serviceStateListener;
	
	private TimeExchangeEventEmitter timeEventEmitter;
	
	private DelayTaskActivator taskActivator;
	private List<Runnable> delayedTaskList = new ArrayList<Runnable>();
	
	public DefaultFileObjectSynchronier(DiskNodeConnectionPool connectionPool,
			ServiceManager serviceManager,
			TimeExchangeEventEmitter timeEventEmitter,
			FilePathMaker pathMaker) {
		this(DEFAULT_THREAD_NUM, connectionPool, serviceManager, timeEventEmitter, pathMaker);
	}
	
	public DefaultFileObjectSynchronier(int threadNum,
			DiskNodeConnectionPool connectionPool,
			ServiceManager serviceManager,
			TimeExchangeEventEmitter timeEventEmitter,
			FilePathMaker pathMaker) {
		this.connectionPool = connectionPool;
		this.pathMaker = pathMaker;
		this.serviceManager = serviceManager;
		this.timeEventEmitter = timeEventEmitter;
		this.serviceStateListener = new DiskNodeServiceStateListener();
		this.taskActivator = new DelayTaskActivator();
		this.threadPool = new ThreadPoolExecutor(threadNum, threadNum,
                0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<Runnable>(),
                new PooledThreadFactory("file_synchronize"));
	}

	@Override
	public void start() throws Exception {
		taskActivator.start();
		serviceManager.addServiceStateListener(Configs.getConfiguration().GetConfig(CommonConfigs.CONFIG_DISK_SERVICE_GROUP_NAME), serviceStateListener);
	}

	@Override
	public void stop() throws Exception {
		serviceManager.removeServiceStateListener(Configs.getConfiguration().GetConfig(CommonConfigs.CONFIG_DISK_SERVICE_GROUP_NAME), serviceStateListener);
		taskActivator.quit();
		taskActivator.interrupt();
		threadPool.shutdown();
	}

	@Override
	public void synchronize(FileObject file, FileObjectSynchronizeCallback callback) {
		threadPool.submit(new FileSynchronizeTask(file, callback));
	}
	
	private void addDelayedTask(FileSynchronizeTask task) {
		LOG.info("add to delayed task list for filnode[{}]", task.fileNode().getName());
		delayedTaskList.add(task);
	}
	
	public class FileSynchronizeTask implements Runnable {
		private final FileObject file;
		private final FileObjectSynchronizeCallback callback;
		
		public FileSynchronizeTask(FileObject file, FileObjectSynchronizeCallback callback) {
			this.file = file;
			this.callback = callback;
		}
		
		public FileNode fileNode() {
			return file.node();
		}

		@Override
		public void run() {
			LOG.info("start to synchronize file[{}]", file.node().getName());
			DuplicateNode[] nodeList = file.node().getDuplicateNodes();
			
			boolean syncAccomplished = true;
			List<DuplicateLength> fileLengthList = getFileLengthList(nodeList);
			
			if(fileLengthList.isEmpty()) {
				//文件所在的所有磁盘节点都处于异常状态
				LOG.error("No available duplicate node is found to sync file[{}]", file.node().getName());
				addDelayedTask(this);
				return;
			}
			
			if(fileLengthList.size() != nodeList.length) {
				//文件所在的所有磁盘节点中有部分不可用，这种情况先同步可用的磁盘节点信息
				LOG.warn("Not all duplicate nodes are available to sync file[{}]", file.node().getName());
				syncAccomplished = false;
			}
			
			long maxLength = -1;
			for(DuplicateLength length : fileLengthList) {
				maxLength = Math.max(maxLength, length.getFileLength());
			}
			
			List<DuplicateNode> lacks = new ArrayList<DuplicateNode>();
			List<DuplicateNode> fulled = new ArrayList<DuplicateNode>();
			for(DuplicateLength length : fileLengthList) {
				if(length.getFileLength() != maxLength) {
					lacks.add(length.getNode());
				} else {
					fulled.add(length.getNode());
				}
			}
			
			if(lacks.isEmpty()) {
				LOG.info("file[{}] is ok!", file.node().getName());
				if(syncAccomplished) {
					callback.complete(file, maxLength);
				} else {
					addDelayedTask(this);
				}
				
				return;
			}
			
			//TODO
			//TODO
			syncAccomplished &= doSynchronize(maxLength, lacks, fulled);
			if(syncAccomplished) {
				callback.complete(file, maxLength);
			} else {
				addDelayedTask(this);
			}
			
			LOG.info("End synchronize file[{}]", file.node().getName());
		}
		
		private List<DuplicateLength> getFileLengthList(DuplicateNode[] nodeList) {
			List<DuplicateLength> fileLengthList = new ArrayList<DuplicateLength>();
			
			for(DuplicateNode node : nodeList) {
				DiskNodeConnection connection = connectionPool.getConnection(node);
				if(connection == null || connection.getClient() == null) {
					LOG.error("duplication node[{}, {}] of [{}] is not available, that's maybe a trouble!", node.getGroup(), node.getId(), file.node().getName());
					continue;
				}
				
				String filePath = pathMaker.buildPath(file.node(), node);
				LOG.info("checking---{}", filePath);
				long fileLength = connection.getClient().getFileLength(filePath);
				
				if(fileLength < 0) {
					LOG.error("duplication node[{}, {}] of [{}] can not get file length, that's maybe a trouble!", node.getGroup(), node.getId(), file.node().getName());
					continue;
				}
				
				LOG.info("server{} -- {}", node.getId(), fileLength);
				
				DuplicateLength nodeSequence = new DuplicateLength();
				nodeSequence.setNode(node);
				nodeSequence.setFileLength(fileLength);
				
				fileLengthList.add(nodeSequence);
			}
			
			return fileLengthList;
		}
		
		private boolean doSynchronize(long correctLength, List<DuplicateNode> lacks, List<DuplicateNode> fulls) {
			//TODO
			List<String> serviceList = new ArrayList<String>();
			for(DuplicateNode node : fulls) {
				serviceList.add(node.getId());
			}
			
			boolean allSynced = true;
			for(DuplicateNode node : lacks) {
				DiskNodeConnection connection = connectionPool.getConnection(node);
				if(connection == null || connection.getClient() == null) {
					LOG.error("can not recover file[{}], because of lack of connection to duplication node[{}]", file.node().getName(), node);
					allSynced = false;
					continue;
				}
				
				DiskNodeClient client = connection.getClient();
				if(client == null) {
					allSynced = false;
					continue;
				}
				
				LOG.info("start synchronize file[{}] at duplicate node[{}]", file.node().getName(), node);
				if(!client.recover(pathMaker.buildPath(file.node(), node), correctLength, serviceList)) {
					LOG.error("can not synchronize file[{}] at duplicate node[{}]", file.node().getName(), node);
					allSynced = false;
				}
				
			}
			
			return allSynced;
		}
	}
	
	private class DiskNodeServiceStateListener implements ServiceStateListener {

		@Override
		public void serviceAdded(Service service) {
			LOG.info("Disk Node[{}] added", service);
			taskActivator.putService(service);
		}

		@Override
		public void serviceRemoved(Service service) {
			LOG.info("Disk Node[{}] removed", service);
		}
		
	}
	
	private class DelayTaskActivator extends Thread {
		private BlockingQueue<Service> serviceQueue = new LinkedBlockingQueue<Service>();
		
		private static final int POLL_TIMEOUT_MILLIS = 30 * 1000;
		
		private volatile boolean isQuit = false;
		
		public void putService(Service service) {
			try {
				serviceQueue.put(service);
			} catch (InterruptedException e) {
				LOG.error("put service error", e);
			}
		}
		
		private String nodeToken(DuplicateNode node) {
			StringBuilder builder = new StringBuilder();
			builder.append(node.getGroup()).append("_").append(node.getId());
			
			return builder.toString();
		}
		
		public void quit() {
			isQuit = true;
		}

		@Override
		public void run() {
			while(!isQuit) {
				try {
					serviceQueue.poll(POLL_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
					
					FileSynchronizeTask[] tasks;
					synchronized (delayedTaskList) {
						tasks = new FileSynchronizeTask[delayedTaskList.size()];
						delayedTaskList.toArray(tasks);
						delayedTaskList.clear();
					}
					
					LOG.info("start to check vadility of {} delayed tasks", tasks.length);
					if(tasks.length == 0) {
						continue;
					}
					
					Map<String, Boolean> serviceStates = new HashMap<String, Boolean>();
					for(FileSynchronizeTask task : tasks) {
						FileNode fileNode = task.fileNode();
						
						boolean canBeSubmitted = true;
						for(DuplicateNode node : fileNode.getDuplicateNodes()) {
							String nodeToken = nodeToken(node);
							Boolean exist = serviceStates.get(nodeToken);
							if(exist == null) {
								exist = serviceManager.getServiceById(node.getGroup(), node.getId()) != null;
								serviceStates.put(nodeToken, exist);
							}
							
							if(!exist) {
								canBeSubmitted = false;
								break;
							}
						}
						
						if(canBeSubmitted) {
							LOG.info("restart task for fileNode[{}]", fileNode.getName());
							threadPool.submit(task);
							continue;
						}
						
						addDelayedTask(task);
						LOG.info("skipped synchronize file node[{}]", fileNode.getName());
					}
				} catch (Exception e) {
					if(!isQuit) {
						LOG.error("task activator error", e);
					}
				}
			}
		}
		
	}

	@Override
	public void timeExchanged(long startTime, Duration duration) {
		// TODO Auto-generated method stub
		
	}

}
