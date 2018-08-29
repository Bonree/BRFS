package com.bonree.brfs.duplication.filenode.duplicates;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCache.StartMode;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent.Type;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bonree.brfs.common.service.Service;
import com.bonree.brfs.common.service.ServiceManager;
import com.bonree.brfs.common.utils.BrStringUtils;
import com.bonree.brfs.common.utils.JsonUtils;
import com.bonree.brfs.common.utils.Pair;
import com.bonree.brfs.configuration.Configs;
import com.bonree.brfs.configuration.units.CommonConfigs;
import com.bonree.brfs.duplication.datastream.connection.DiskNodeConnection;
import com.bonree.brfs.duplication.datastream.connection.DiskNodeConnectionPool;
import com.bonree.brfs.resourceschedule.model.ResourceModel;
import com.bonree.brfs.resourceschedule.service.AvailableServerInterface;

public class ResourceDuplicateNodeSelector implements DuplicateNodeSelector {
	private static final Logger LOG = LoggerFactory.getLogger(ResourceDuplicateNodeSelector.class);
	private String zkPath = null;
	private AvailableServerInterface available = null;
	private CuratorFramework client = null;
	private PathChildrenCache pathCache = null;
	private ServiceManager serviceManager = null;
	private DiskNodeConnectionPool connectionPool = null;
	private Random rand = new Random();
	private int centSize = 1000;
	public ResourceDuplicateNodeSelector(int centSize, String zkPath,ServiceManager serviceManager, DiskNodeConnectionPool connectionPool,CuratorFramework client,AvailableServerInterface available) {
		this.centSize = centSize <=0 ? 1000: centSize;
		this.zkPath = zkPath;
		this.serviceManager = serviceManager;
		this.connectionPool = connectionPool;
		this.available = available;
		this.client = client;
		this.pathCache = new PathChildrenCache(client, zkPath, true);
	}
	public ResourceDuplicateNodeSelector() {
	}
	
	@Override
	public DuplicateNode[] getDuplicationNodes(int storageId, int nums) {
		String groupName = Configs.getConfiguration().GetConfig(CommonConfigs.CONFIG_DATA_SERVICE_GROUP_NAME);
		List<Service> serviceList = serviceManager.getServiceListByGroup(groupName);
		if(serviceList == null || serviceList.isEmpty()) {
			LOG.error("[{}] select service list is empty !!!!", groupName);
			return new DuplicateNode[0];
		}
		List<Pair<String, Integer>> servers = null;
		try {
			long select1 = System.currentTimeMillis();
			servers = this.available.selectAvailableServers(1, storageId, null, centSize);
			long select2 = System.currentTimeMillis();
			LOG.info("select services time [{}]", select2 - select1);
			
		} catch (Exception e) {
			LOG.error("{}",e);
		}
		LOG.info("disk group : {}, services:{}",groupName,servers);
		// 若过滤不出正常的则采用随机
		DuplicateNode[] dups = null;
		long start = System.currentTimeMillis();
		if(servers !=null && !servers.isEmpty()) {
			dups = getResource(storageId, nums, servers);
		}
		if(dups == null || dups.length ==0) {
			dups = getRandom(storageId, nums);
		}
		long end = System.currentTimeMillis();
		LOG.info("[DUP] select Times [{}]ms dups size {}", end - start, dups.length);
		return dups;
	}
	private DuplicateNode[] getResource(int storageId, int nums,List<Pair<String,Integer>> servers) {
		List<Service> serviceList = serviceManager.getServiceListByGroup(Configs.getConfiguration().GetConfig(CommonConfigs.CONFIG_DATA_SERVICE_GROUP_NAME));
		if(serviceList.isEmpty()) {
			LOG.error("[{}] resource select serviceList is empty !!!",Configs.getConfiguration().GetConfig(CommonConfigs.CONFIG_DATA_SERVICE_GROUP_NAME));
			return new DuplicateNode[0];
		}
		Map<String,Service> map = new HashMap<String,Service>();
		for(Service server: serviceList) {
			map.put(server.getServiceId(), server);
		}
		String group = serviceList.get(0).getServiceGroup();
		List<DuplicateNode> nodes = new ArrayList<DuplicateNode>();
		List<String> uNeeds = new ArrayList<String>();
		long start = System.currentTimeMillis();
		while(!serviceList.isEmpty() && nodes.size() <nums) {
			String serverId = WeightRandomPattern.getWeightRandom(servers, rand, uNeeds);
			Service service = null;
			
			if(BrStringUtils.isEmpty(serverId)&&!serviceList.isEmpty()) {
				service = serviceList.remove(rand.nextInt(serviceList.size()));
			}else {
				uNeeds.add(serverId);
				service = map.get(serverId);
			}
			if(service == null) {
				continue;
			}
			DuplicateNode node = new DuplicateNode(service.getServiceGroup(), service.getServiceId());
			DiskNodeConnection conn = connectionPool.getConnection(node.getGroup(), node.getId());
			if(conn == null || conn.getClient() == null) {
				continue;
			}
			serviceList.remove(service);
			nodes.add(node);
		}
		DuplicateNode[] result = new DuplicateNode[nodes.size()];
		long end = System.currentTimeMillis();
		LOG.info("select time {} ms, select services :{}",(end - start),uNeeds);
		return nodes.toArray(result);
	}
	/**
	 * 概述：随机返回数据节点
	 * @param storageId
	 * @param nums
	 * @return
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	private DuplicateNode[] getRandom(int storageId, int nums) {
		List<Service> serviceList = serviceManager.getServiceListByGroup(Configs.getConfiguration().GetConfig(CommonConfigs.CONFIG_DATA_SERVICE_GROUP_NAME));
		if(serviceList.isEmpty()) {
			LOG.info("[DUP] seviceList is empty");
			return new DuplicateNode[0];
		}
		List<DuplicateNode> nodes = new ArrayList<DuplicateNode>();
		while(!serviceList.isEmpty() && nodes.size() < nums) {
			Service service = serviceList.remove(rand.nextInt(serviceList.size()));
			DuplicateNode node = new DuplicateNode(service.getServiceGroup(), service.getServiceId());
			DiskNodeConnection conn = connectionPool.getConnection(node.getGroup(), node.getId());
			if(conn == null || conn.getClient() == null) {
				continue;
			}
			nodes.add(node);
		}
		
		DuplicateNode[] result = new DuplicateNode[nodes.size()];
		return nodes.toArray(result);
	}
	/**
	 * 概述：启动监听
	 * @return
	 * @throws Exception
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	public ResourceDuplicateNodeSelector start() throws Exception {
		ExecutorService pool = Executors.newSingleThreadExecutor();
		pathCache.start(StartMode.BUILD_INITIAL_CACHE);
		pathCache.getListenable().addListener(new PathChildrenCacheListener() {
			@Override
			public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception {
				Type type = event.getType();
				ChildData data = event.getData();
				if(data == null){
					LOG.warn("Event : {} ,ChildData is null",type);
					return;
				}
				byte[] content = data.getData();
				if(content == null || content.length == 0){
					LOG.warn("Event : {} ,Byte data is null",type);
					return;
				}
				ResourceModel resource = JsonUtils.toObjectQuietly(content, ResourceModel.class);
				if(resource == null){
					LOG.warn("Event : {} , Convert data is null",type);
					return ;
				}
				String str = JsonUtils.toJsonString(resource);
				if(Type.CHILD_ADDED == type) {
					available.add(resource);
				}else if(Type.CHILD_REMOVED == type) {
					available.remove(resource);
				}else if(Type.CHILD_UPDATED == type) {
					available.update(resource);
				}else {
					LOG.warn("event : {}, content:{}",type,str);
				}
			}
		}, pool);
		return this;
	}
	/**
	 * 概述：停止监听
	 * @return
	 * @throws IOException
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	public ResourceDuplicateNodeSelector close() throws IOException {
		pathCache.close();
		client.close();
		return this;
	}
	
	public ResourceDuplicateNodeSelector setClient(CuratorFramework client,String zkPath) {
		this.client =client;
		this.pathCache = new PathChildrenCache(client, zkPath, true);
		return this;
	}

	public ResourceDuplicateNodeSelector setAvailable(AvailableServerInterface available) {
		this.available = available;
		return this;
	}

	public ResourceDuplicateNodeSelector setServiceManager(ServiceManager serviceManager) {
		this.serviceManager = serviceManager;
		return this;
	}

	public ResourceDuplicateNodeSelector setConnectionPool(DiskNodeConnectionPool connectionPool) {
		this.connectionPool = connectionPool;
		return this;
	}
	public ResourceDuplicateNodeSelector setZkPath(String zkPath) {
		this.zkPath = zkPath;
		return this;
	}
	public ResourceDuplicateNodeSelector setCentSize(int centSize) {
		this.centSize = centSize <= 0 ? 1000 : centSize;
		return this;
	}
}
