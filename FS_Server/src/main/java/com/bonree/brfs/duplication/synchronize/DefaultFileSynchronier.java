package com.bonree.brfs.duplication.synchronize;

import java.io.File;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bonree.brfs.common.service.Service;
import com.bonree.brfs.common.service.ServiceManager;
import com.bonree.brfs.common.service.ServiceStateListener;
import com.bonree.brfs.common.utils.BitSetUtils;
import com.bonree.brfs.common.utils.PooledThreadFactory;
import com.bonree.brfs.configuration.Configs;
import com.bonree.brfs.configuration.units.DiskNodeConfigs;
import com.bonree.brfs.configuration.units.DuplicateNodeConfigs;
import com.bonree.brfs.disknode.client.AvailableSequenceInfo;
import com.bonree.brfs.disknode.client.DiskNodeClient;
import com.bonree.brfs.disknode.client.RecoverInfo;
import com.bonree.brfs.duplication.DuplicationEnvironment;
import com.bonree.brfs.duplication.coordinator.DuplicateNode;
import com.bonree.brfs.duplication.coordinator.FileNode;
import com.bonree.brfs.duplication.coordinator.FilePathBuilder;
import com.bonree.brfs.duplication.datastream.connection.DiskNodeConnection;
import com.bonree.brfs.duplication.datastream.connection.DiskNodeConnectionPool;
import com.bonree.brfs.server.identification.ServerIDManager;

public class DefaultFileSynchronier implements FileSynchronizer {
	private static final Logger LOG = LoggerFactory.getLogger(DefaultFileSynchronier.class);
	
	private static final int DEFAULT_THREAD_NUM = 1;
	private ExecutorService threadPool;
	private ScheduledExecutorService scheduledPool;
	
	private DiskNodeConnectionPool connectionPool;
	
	private ServerIDManager idManager;
	
	private ServiceManager serviceManager;
	private ServiceStateListener serviceStateListener;
	
	private DelayTaskActivator taskActivator;
	private List<Runnable> delayedTaskList = new ArrayList<Runnable>();
	
	private Map<String, Future<?>> syncTasks = new HashMap<String, Future<?>>();
	
	private static final String ERROR_FILE = "error_records";                         
	private SynchronierErrorRecorder errorRecorder;
	
	public DefaultFileSynchronier(DiskNodeConnectionPool connectionPool, ServiceManager serviceManager, ServerIDManager idManager) {
		this(DEFAULT_THREAD_NUM, connectionPool, serviceManager, idManager);
	}
	
	public DefaultFileSynchronier(int threadNum, DiskNodeConnectionPool connectionPool, ServiceManager serviceManager, ServerIDManager idManager) {
		this.connectionPool = connectionPool;
		this.idManager = idManager;
		this.serviceManager = serviceManager;
		this.serviceStateListener = new DiskNodeServiceStateListener();
		this.taskActivator = new DelayTaskActivator();
		this.errorRecorder = new SynchronierErrorRecorder(
				new File(Configs.getConfiguration().GetConfig(DuplicateNodeConfigs.CONFIG_LOG_DIR_PATH), ERROR_FILE));
		this.threadPool = new ThreadPoolExecutor(threadNum, threadNum,
                0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<Runnable>(),
                new PooledThreadFactory("file_synchronize"));
		this.scheduledPool = new ScheduledThreadPoolExecutor(1,
				new PooledThreadFactory("delayed_file_sync"));
	}

	@Override
	public void start() throws Exception {
		taskActivator.start();
		serviceManager.addServiceStateListener(Configs.getConfiguration().GetConfig(DiskNodeConfigs.CONFIG_SERVICE_GROUP_NAME), serviceStateListener);
	}

	@Override
	public void stop() throws Exception {
		serviceManager.removeServiceStateListener(Configs.getConfiguration().GetConfig(DiskNodeConfigs.CONFIG_SERVICE_GROUP_NAME), serviceStateListener);
		taskActivator.quit();
		taskActivator.interrupt();
		threadPool.shutdown();
		scheduledPool.shutdown();
	}
	
	@Override
	public void synchronize(FileNode fileNode, FileSynchronizeCallback callback, long delayedTime, TimeUnit unit) {
		scheduledPool.schedule(new Runnable() {
			
			@Override
			public void run() {
				synchronize(fileNode, callback);
			}
		}, delayedTime, unit);
	}

	@Override
	public void synchronize(FileNode fileNode, FileSynchronizeCallback callback) {
		synchronized(syncTasks) {
			Future<?> taskFuture = syncTasks.get(fileNode.getName());
			if(taskFuture != null) {
				taskFuture.cancel(false);
			}
			
			syncTasks.put(fileNode.getName(), threadPool.submit(new FileSynchronizeTask(fileNode, callback)));
		}
	}
	
	private void addDelayedTask(FileSynchronizeTask task) {
		LOG.info("add to delayed task list for filnode[{}]", task.fileNode().getName());
		delayedTaskList.add(task);
	}
	
	public class FileSynchronizeTask implements Runnable {
		private final FileNode target;
		private final FileSynchronizeCallback callback;
		
		public FileSynchronizeTask(FileNode fileNode, FileSynchronizeCallback callback) {
			this.target = fileNode;
			this.callback = callback;
		}
		
		public FileNode fileNode() {
			return this.target;
		}
		
		private void triggerCallbackComplete() {
			callback.complete(target);
		}
		
		private void triggerCallbackError(Throwable cause) {
			callback.error(cause);
		}

		@Override
		public void run() {
			// 运行任务前先删除对应的future，因为任务开始执行后，它就没有留在Map中的理由了
			synchronized(syncTasks) {
				syncTasks.remove(target.getName());
			}
			
			LOG.info("start synchronize file[{}]", target.getName());
			
			/**
			 * 文件之间的内容协调是通过写入文件的序列号实现的，只要当前存活的磁盘节点包含
			 * 所有写入序列号就能保证文件的完整性
			 */
			List<DuplicateNode> nodeList = getActiveDuplicateNodes(target);
			
			List<DuplicateNodeSequence> seqNumberList = getAllDuplicateNodeSequence(nodeList);
			
			if(seqNumberList.size() != nodeList.size()) {
				//不相等的情况不能再继续恢复了，我们选择等待
				LOG.info("can not get all sequences of file[{}]", target.getName());
				addDelayedTask(this);
				return;
			}
			
			if(seqNumberList.isEmpty()) {
				LOG.error("No available duplicate node to found for file[{}]", target.getName());
				addDelayedTask(this);
				return;
			}
			
			BitSet[] sets = new BitSet[seqNumberList.size()];
			for(int i = 0; i < sets.length; i++) {
				sets[i] = seqNumberList.get(i).getSequenceNumbers();
			}
			
			//每个副本节点的序列号的交集
			BitSet union = BitSetUtils.union(sets);
			//每个副本节点都有的序列号
			BitSet intersection = BitSetUtils.intersect(sets);
			
			/**
			 * 查看所有节点的序列号是否覆盖了[0, maxSeq]之间的所有数值
			 */
			LOG.info("synchronize Check report union[{}], itersection[{}]", union.cardinality(), intersection.cardinality());
			if(union.nextSetBit(union.cardinality()) == -1) {
				//当前存活的所有节点包含了此文件的所有信息，可以进行文件内容同步
				if(intersection.cardinality() == union.cardinality()) {
					//如果交集和并集的数量一样，说明没有文件数据缺失
					LOG.info("file[{}] is ok!", target.getName());
					triggerCallbackComplete();
					return;
				}
				
				doSynchronize(seqNumberList, union, intersection);
			} else {
				//存在文件内容丢失，怎么办，怎么办
				LOG.info("file[{}] is error!", target.getName());
				errorRecorder.writeErrorFile(target);
				triggerCallbackError(new Exception("Content of the file[" + target.getName() + "] is deficient!!"));
			}
			
			LOG.info("End synchronize file[{}]", target.getName());
		}
		
		private List<DuplicateNode> getActiveDuplicateNodes(FileNode fileNode) {
			List<DuplicateNode> nodeList = new ArrayList<DuplicateNode>();
			for(DuplicateNode node : fileNode.getDuplicateNodes()) {
				if(node.getGroup().equals(DuplicationEnvironment.VIRTUAL_SERVICE_GROUP)) {
					LOG.info("virtual server[{}, {}] is ignored to get sequences", node.getGroup(), node.getId());
					continue;
				}
				
				nodeList.add(node);
			}
			
			return nodeList;
		}
		
		private List<DuplicateNodeSequence> getAllDuplicateNodeSequence(List<DuplicateNode> nodeList) {
			List<DuplicateNodeSequence> seqNumberList = new ArrayList<DuplicateNodeSequence>();
			
			for(DuplicateNode node : nodeList) {
				DiskNodeConnection connection = connectionPool.getConnection(node);
				if(connection == null || connection.getClient() == null) {
					LOG.error("duplication node[{}, {}] of [{}] is not available, that's maybe a trouble!", node.getGroup(), node.getId(), target.getName());
					continue;
				}
				
				String serverId = idManager.getOtherSecondID(node.getId(), target.getStorageId());
				String filePath =FilePathBuilder.buildFilePath(target.getStorageName(), serverId, target.getCreateTime(), target.getName());
				LOG.info("checking---{}", filePath);
				BitSet seqNumbers = connection.getClient().getWritingSequence(filePath);
				
				if(seqNumbers == null) {
					LOG.error("duplication node[{}, {}] of [{}] can not get file sequences, that's maybe a trouble!", node.getGroup(), node.getId(), target.getName());
					continue;
				}
				
				LOG.info("server{} -- {}", node.getId(), seqNumbers.cardinality());
				
				DuplicateNodeSequence nodeSequence = new DuplicateNodeSequence();
				nodeSequence.setNode(node);
				nodeSequence.setSequenceNumbers(seqNumbers);
				
				seqNumberList.add(nodeSequence);
			}
			
			return seqNumberList;
		}
		
		private void doSynchronize(List<DuplicateNodeSequence> seqNumberList, BitSet union, BitSet intersection) {
			List<AvailableSequenceInfo> infos = new ArrayList<AvailableSequenceInfo>();
			for(DuplicateNodeSequence sequence : seqNumberList) {
				BitSet iHave = BitSetUtils.minus(sequence.getSequenceNumbers(), intersection);
				LOG.info("Duplicate node[{}] has available seq num[{}]", sequence.getNode(), iHave.cardinality());
				
				AvailableSequenceInfo info = new AvailableSequenceInfo();
				info.setServiceGroup(sequence.getNode().getGroup());
				info.setServiceId(sequence.getNode().getId());
				info.setAvailableSequence(iHave);
				
				String serverId = idManager.getOtherSecondID(sequence.getNode().getId(), target.getStorageId());
				info.setFilePath(FilePathBuilder.buildFilePath(target.getStorageName(), serverId, target.getCreateTime(), target.getName()));
				
				infos.add(info);
			}
			
			//这部分我们称呼它为“补漏”
			RecoverInfo recoverInfo = new RecoverInfo();
			recoverInfo.setMaxSeq(union.cardinality() - 1);
			recoverInfo.setInfoList(infos);
			for(DuplicateNodeSequence sequence : seqNumberList) {
				BitSet lack = BitSetUtils.minus(union, sequence.getSequenceNumbers());
				if(lack.isEmpty()) {
					//没有空缺内容，不需要恢复
					continue;
				}
				
				DiskNodeConnection connection = connectionPool.getConnection(sequence.getNode());
				if(connection == null || connection.getClient() == null) {
					LOG.error("can not recover file[{}], because of lack of connection to duplication node[{}]", target.getName(), sequence.getNode());
					continue;
				}
				
				DiskNodeClient client = connection.getClient();
				String serverId = idManager.getOtherSecondID(sequence.getNode().getId(), target.getStorageId());
				LOG.info("start synchronize file[{}] at duplicate node[{}]", target.getName(), sequence.getNode());
				if(!client.recover(FilePathBuilder.buildFilePath(target.getStorageName(), serverId, target.getCreateTime(), target.getName()), recoverInfo)) {
					LOG.error("can not synchronize file[{}] at duplicate node[{}]", target.getName(), sequence.getNode());
					addDelayedTask(this);
					return;
				}
				LOG.info("file synchronizition completed successfully!");
			}
			
			triggerCallbackComplete();
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
							if(node.getGroup().equals(DuplicationEnvironment.VIRTUAL_SERVICE_GROUP)) {
								continue;
							}
							
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
					LOG.error("task activator error", e);
				}
			}
		}
		
	}

}
