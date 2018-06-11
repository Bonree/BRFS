package com.bonree.brfs.duplication.coordinator.zk;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.curator.framework.CuratorFramework;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bonree.brfs.common.service.Service;
import com.bonree.brfs.common.service.ServiceManager;
import com.bonree.brfs.common.service.ServiceStateListener;
import com.bonree.brfs.common.utils.JsonUtils;
import com.bonree.brfs.configuration.ServerConfig;
import com.bonree.brfs.duplication.coordinator.FileNode;
import com.bonree.brfs.duplication.coordinator.FileNodeSinkSelector;
import com.bonree.brfs.duplication.coordinator.FileNodeStorer;

/**
 * 把已消失的Service管理的FileNode分配到其他可用Service中，保证
 * 文件节点可以继续写入
 * 
 * @author yupeng
 *
 */
class FileNodeDistributor implements Runnable, ServiceStateListener {
	private static final Logger LOG = LoggerFactory.getLogger(FileNodeDistributor.class);
	
	private LinkedBlockingQueue<Service> downServiceList = new LinkedBlockingQueue<Service>();
	private ConcurrentHashMap<String, Long> serviceActiveTimes = new ConcurrentHashMap<String, Long>();
	
	private FileNodeSinkSelector serviceSelector;
	
	private CuratorFramework client;
	private FileNodeStorer fileStorer;
	private ServiceManager serviceManager;
	
	private volatile boolean isQuit = false;
	
	private AtomicBoolean runningState = new AtomicBoolean(false);
	private Thread currentThread;
	
	public FileNodeDistributor(CuratorFramework client, FileNodeStorer fileStorer, ServiceManager serviceManager, FileNodeSinkSelector serviceSelector) {
		this.client = client;
		this.fileStorer = fileStorer;
		this.serviceManager = serviceManager;
		this.serviceSelector = serviceSelector;
	}
	
	public boolean isStarted() {
		return runningState.get();
	}
	
	public void quit() {
		isQuit = true;
		if(currentThread != null) {
			currentThread.interrupt();
		}
	}
	
	//检测出所有失效的文件节点，并对其进行转移
	private void handleInvalidFileNode() {
		for(Service service : serviceManager.getServiceListByGroup(ServerConfig.DEFAULT_DUPLICATION_SERVICE_GROUP)) {
			serviceActiveTimes.put(serviceToken(service.getServiceGroup(), service.getServiceId()), service.getRegisterTime());
		}
		
		//任务开始前需要先进行文件扫描，确定需要转移的文件
		for(FileNode fileNode : fileStorer.listFileNodes(new IdentityFileNodeFilter())) {
			long serviceAddTime = serviceActiveTimes.getOrDefault(serviceToken(fileNode.getServiceGroup(), fileNode.getServiceId()), Long.MAX_VALUE);
			if(serviceAddTime > fileNode.getServiceTime()) {
				LOG.info("transfer file node[{}]", fileNode.getName());
				transferFileNode(fileNode);
			}
		}
	}
	
	private void transferFileNode(FileNode fileNode) {
		Service target = serviceSelector.selectWith(fileNode, serviceManager.getServiceListByGroup(ServerConfig.DEFAULT_DUPLICATION_SERVICE_GROUP));
		LOG.info("transfer fileNode[{}] to service[{}]", fileNode.getName(), target.getServiceId());
		
		try {
			fileNode.setServiceId(target.getServiceId());
			fileNode.setServiceTime(target.getRegisterTime());
			fileStorer.update(fileNode);

			// 在Sink中放入分配的文件名
			client.create().creatingParentsIfNeeded().forPath(ZkFileCoordinatorPaths.buildSinkFileNodePath(fileNode), JsonUtils.toJsonBytes(fileNode));
		} catch(Exception e) {
			//TODO 处理转移失败的文件
			LOG.error("transfer file[{}] error", fileNode.getName());
		}
	}

	@Override
	public void run() {
		if(!runningState.compareAndSet(false, true)) {
			//程序已经处于启动状态
			return;
		}
		
		LOG.info("starting...");
		currentThread = Thread.currentThread();
		
		//先对所有失效文件进行处理
		handleInvalidFileNode();
		
		try {
			while(!isQuit) {
				try {
					Service service = downServiceList.take();
					
					for (FileNode node : fileStorer.listFileNodes(new ServiceFileNodeFilter(service))) {
						transferFileNode(node);
					}
				} catch (Exception e) {
					LOG.error("transfer files error", e);
				}
			}
		} finally {
			isQuit = false;
			runningState.set(false);
		}
	}
	
	private String serviceToken(String group, String id) {
		StringBuilder builder = new StringBuilder();
		builder.append(group).append("_").append(id);
		
		return builder.toString();
	}

	@Override
	public void serviceAdded(Service service) {
		LOG.info("Service added#######{}", service.getServiceId());
		serviceActiveTimes.put(serviceToken(service.getServiceGroup(), service.getServiceId()), service.getRegisterTime());
	}

	@Override
	public void serviceRemoved(Service service) {
		LOG.info("Service removed#######{}", service.getServiceId());
		serviceActiveTimes.remove(serviceToken(service.getServiceGroup(), service.getServiceId()));
		//删除服务对应的文件槽
		try {
			client.delete()
			      .quietly()
				  .deletingChildrenIfNeeded()
				  .forPath(ZkFileCoordinatorPaths.buildSinkPath(service));
		} catch (Exception e) {
			LOG.warn("Can not delete the sink of crushed service[{}]", service.getServiceId(), e);
		}
		
		//把崩溃的Service信息放入队列进行处理
		try {
			downServiceList.put(service);
		} catch (InterruptedException e) {
			LOG.error("put down service error", e);
		}
	}
}
