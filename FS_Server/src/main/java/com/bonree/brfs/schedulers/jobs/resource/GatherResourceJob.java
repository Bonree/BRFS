
package com.bonree.brfs.schedulers.jobs.resource;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryNTimes;
import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import org.quartz.UnableToInterruptJobException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bonree.brfs.common.service.Service;
import com.bonree.brfs.common.service.ServiceManager;
import com.bonree.brfs.common.service.impl.DefaultServiceManager;
import com.bonree.brfs.common.utils.BrStringUtils;
import com.bonree.brfs.common.utils.JsonUtils;
import com.bonree.brfs.common.zookeeper.ZookeeperClient;
import com.bonree.brfs.common.zookeeper.curator.CuratorClient;
import com.bonree.brfs.duplication.storagename.DefaultStorageNameManager;
import com.bonree.brfs.duplication.storagename.StorageNameManager;
import com.bonree.brfs.duplication.storagename.StorageNameNode;
import com.bonree.brfs.resourceschedule.commons.GatherResource;
import com.bonree.brfs.resourceschedule.model.BaseMetaServerModel;
import com.bonree.brfs.resourceschedule.model.ResourceModel;
import com.bonree.brfs.resourceschedule.model.ServerModel;
import com.bonree.brfs.resourceschedule.model.StatServerModel;
import com.bonree.brfs.resourceschedule.model.StateMetaServerModel;
import com.bonree.brfs.resourceschedule.service.AvailableServerInterface;
import com.bonree.brfs.schedulers.ManagerContralFactory;
import com.bonree.brfs.schedulers.jobs.JobDataMapConstract;
import com.bonree.brfs.schedulers.task.manager.RunnableTaskInterface;
import com.bonree.brfs.schedulers.task.operation.impl.QuartzOperationStateTask;

public class GatherResourceJob extends QuartzOperationStateTask {
	private static final Logger LOG = LoggerFactory.getLogger("GATHER");
	private static Queue<StateMetaServerModel> queue = new ConcurrentLinkedQueue<StateMetaServerModel>();
	private static StateMetaServerModel prexState = null;

	@Override
	public void caughtException(JobExecutionContext context) {

	}

	@Override
	public void interrupt() throws UnableToInterruptJobException {
		LOG.info("Interrupt job :", this.getClass().getName());
	}

	@Override
	public void operation(JobExecutionContext context) throws Exception {
		JobDataMap data = context.getJobDetail().getJobDataMap();
		if (data == null || data.isEmpty()) {
			throw new NullPointerException("job data map is empty");
		}
		String dataDir = data.getString(JobDataMapConstract.DATA_PATH);
		// TODO:若是设置的为host，此处需要进行host转ip
		String ip = data.getString(JobDataMapConstract.IP);
		long gatherInveral = data.getLongValueFromString(JobDataMapConstract.GATHER_INVERAL_TIME);
		int count = data.getIntFromString(JobDataMapConstract.CALC_RESOURCE_COUNT);
		StateMetaServerModel metaSource = GatherResource.gatherResource(dataDir, ip);
		if (metaSource != null) {
			queue.add(metaSource);
			LOG.info("gather stat info !!! {}", queue.size());
			
		}
		int queueSize = queue.size();
		if (queueSize >= count) {
			updateResource(dataDir, gatherInveral);

		}
	}
	/***
	 * 概述：更新资源
	 * @param metaSource
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	public void updateResourceOfTask(final StateMetaServerModel metaSource){
		if(metaSource == null){
			return;
		}
		// 更新任务可执行接口资源信息
		if (prexState == null) {
			prexState = metaSource;
		}else {
			StatServerModel stat = metaSource.converObject(prexState);
			ManagerContralFactory mcf = ManagerContralFactory.getInstance();
			RunnableTaskInterface run = mcf.getRt();
			if(run == null){
				LOG.warn("RunnableTaskInterface is null");
			}
			run.update(stat);
			prexState = metaSource;
			LOG.info("update RunnableTaskInterface state !!!");
			LOG.info("state : {}",JsonUtils.toJsonString(stat));
		}
	}
	/***
	 * 概述：更新资源
	 * @param zkUrl
	 * @param groupName
	 * @param serverId
	 * @param dataDir
	 * @param inverTime
	 * @throws Exception
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	private void updateResource(String dataDir, long inverTime) throws Exception {

		StatServerModel sum = calcStateServer(inverTime,dataDir);
		if(sum == null){
			return;
		}
		//打印资源信息
		LOG.info("cpu rate {}",sum.getCpuRate());
		LOG.info("cpu core {}",sum.getCpuCoreCount());
		LOG.info("memorya rate {}",sum.getMemoryRate());
		LOG.info("memory Size {}",sum.getMemorySize());
		LOG.info("Disk size {}",sum.getTotalDiskSize());
		LOG.info("Disk remain Size {}",sum.getRemainDiskSize());
		LOG.info("disk sn remain {}",sum.getPartitionRemainSizeMap());
		LOG.info("net rx {}",sum.getNetRSpeedMap());
		LOG.info("net tx {}", sum.getNetTSpeedMap());
		LOG.info("disk sn w map {}", sum.getPartitionWriteSpeedMap());
		LOG.info("disk sn r map {}", sum.getPartitionReadSpeedMap());
		LOG.info("disk sn total map {}", sum.getPartitionTotalSizeMap());
		LOG.info("sn to disk map :{}",sum.getStorageNameOnPartitionMap());
		// 更新任务可执行接口资源信息
		ManagerContralFactory mcf = ManagerContralFactory.getInstance();
		mcf.getRt().update(sum);
		// 2.计算集群基础基础信息
		BaseMetaServerModel base = getClusterBases();
		if (base == null) {
			LOG.warn("base server info is null !!!");
			return;
		}
		// 7.计算Resource值
		ResourceModel resource = GatherResource.calcResourceValue(base, sum);
		
		if (resource == null) {
			LOG.warn("calc resource value is null !!!");
			return;
		}
		Map<Integer, String> snIds = getStorageNameIdWithName();
		resource.setSnIds(snIds);
		resource.setServerId(mcf.getServerId());
		// 6.获取本机信息
		ServerModel server = getServerModel();
		if(server == null){
			LOG.warn("server model is null !!");
		}
		server.setResource(resource);
		setServerModel(server);
		LOG.info("update zookeeper complete");
	}
	public ServerModel getlocalServerModel(){
		ManagerContralFactory mcf = ManagerContralFactory.getInstance();
		String groupName = mcf.getGroupName();
		String serverId = mcf.getServerId();
		ServiceManager sm = mcf.getSm();
		Service service = sm.getServiceById(groupName, serverId);
		String payload = service.getPayload();
		if(BrStringUtils.isEmpty(payload)){
			return null;
		}
		ServerModel serverModel = JsonUtils.toObject(payload, ServerModel.class);
		return serverModel;
	}
	/**
	 * 概述：获取基本信息
	 * @return
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	private BaseMetaServerModel getClusterBases() {
		ManagerContralFactory mcf = ManagerContralFactory.getInstance();
		String groupName = mcf.getGroupName();
		ServiceManager sManager = mcf.getSm();
		// 1.获取集群基础信息
		List<Service> serverList = sManager.getServiceListByGroup(groupName);
		// 2.计算集群基础基础信息
		BaseMetaServerModel base = calcBaseCluster(serverList);
		return base;
	}

	/**
	 * 概述：计算队列的状态信息
	 * @param inverTime
	 * @param dataDir
	 * @return
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	private StatServerModel calcStateServer(long inverTime, String dataDir) {
		StatServerModel sum = null;
		// 0.计算原始状态信息
		List<StatServerModel> lists = GatherResource.calcState(queue);
		if (lists == null || lists.isEmpty()) {
			LOG.warn("server state list is null !!");
			return sum;
		}
		ManagerContralFactory mcf = ManagerContralFactory.getInstance();
		String groupName = mcf.getGroupName();
		String serverId = mcf.getServerId();

		// 1-1初始化storagename管理器
		// TODO:俞朋 的 获取storageName
		StorageNameManager snManager = mcf.getSnm();
		// 1-2.初始化service管理器
		ServiceManager sManager = mcf.getSm();

		// 2.获取storage信息
		List<StorageNameNode> storageNames = snManager.getStorageNameNodeList();
		List<String> storagenameList = getStorageNames(storageNames);
		// 3.计算状态值
		sum = GatherResource.calcStatServerModel(lists, storagenameList, inverTime, dataDir);
		return sum;
	}

	/**
	 * 概述：检查并创建服务信息
	 * @param content
	 * @param serverId
	 * @param dataDir
	 * @return
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	private ServerModel checkAndCreateServerModel(String content, String serverId, String dataDir) {
		ServerModel sinfo = null;
		if (BrStringUtils.isEmpty(content)) {
			sinfo = new ServerModel();
		}
		sinfo = JsonUtils.toObject(content, ServerModel.class);
		if (sinfo == null) {
			sinfo = new ServerModel();
		}
		BaseMetaServerModel tmpbase = GatherResource.gatherBase(serverId, dataDir);
		sinfo.setBase(tmpbase);
		return sinfo;
	}

	/***
	 * 概述：获取storageName的名称
	 * @param storageNames
	 * @return
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	private List<String> getStorageNames(List<StorageNameNode> storageNames) {
		List<String> snList = new ArrayList<String>();

		if (storageNames == null || storageNames.isEmpty()) {
			return snList;
		}
		String tmp = null;
		for (StorageNameNode sn : storageNames) {
			if (sn == null) {
				continue;
			}
			tmp = sn.getName();
			if (BrStringUtils.isEmpty(tmp)) {
				continue;
			}
			snList.add(tmp);
		}
		return snList;
	}
	

	/**
	 * 概述：计算集群基础信息
	 * @param serverList
	 * @return
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	private BaseMetaServerModel calcBaseCluster(List<Service> serverList) {
		if (serverList == null || serverList.isEmpty()) {
			return null;
		}
		// 2.获取集群基础基础信息
		List<BaseMetaServerModel> bases = new ArrayList<BaseMetaServerModel>();
		BaseMetaServerModel base = null;
		ServerModel sinfo = null;
		String content = null;
		for (Service server : serverList) {
			content = server.getPayload();
			// 2-1.过滤掉为空的
			sinfo = getServerModel(server);
			// 2-2 过滤为null的
			if (sinfo == null) {
				continue;
			}
			base = sinfo.getBase();
			// 2-3 过滤base为null的
			if (base == null) {
				continue;
			}
			bases.add(base);
		}
		if (bases.isEmpty()) {
			return null;
		}
		return GatherResource.collectBaseMetaServer(bases);
	}
	
	public static void  setServerModel(ServerModel server) throws Exception{
		ManagerContralFactory mcf = ManagerContralFactory.getInstance();
		String groupName = mcf.getGroupName();
		String serverId = mcf.getServerId();
		ServiceManager sm = mcf.getSm();
		String payLoad = JsonUtils.toJsonString(server);
		sm.updateService(groupName, serverId, payLoad);
	}
	public static ServerModel getServerModel(){
		ManagerContralFactory mcf = ManagerContralFactory.getInstance();
		String groupName = mcf.getGroupName();
		String serverId = mcf.getServerId();
		return getServerModel(groupName, serverId);
	}
	public static ServerModel getServerModel(String groupName, String serverId){
		ManagerContralFactory mcf = ManagerContralFactory.getInstance();
		ServiceManager sm = mcf.getSm();
		Service service = sm.getServiceById(groupName, serverId);
		return getServerModel(service);
	}
	public static ServerModel getServerModel(Service service){
		if(service == null){
			return null;
		}
		String payLoad = service.getPayload();
		if(BrStringUtils.isEmpty(payLoad)){
			return null;
		}
		return JsonUtils.toObject(payLoad, ServerModel.class);
	}
	/**
	 * 概述：获取storageName关系
	 * @param sns
	 * @return
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	private Map<Integer,String> getStorageNameIdWithName(){
		ManagerContralFactory mcf = ManagerContralFactory.getInstance();
		StorageNameManager snm = mcf.getSnm();
		List<StorageNameNode> sns = snm.getStorageNameNodeList();
		Map<Integer,String> snToId = new ConcurrentHashMap<Integer,String>();
		if(sns == null || sns.isEmpty()){
			return snToId;
		}
		int id = -1;
		String name = null;
		for (StorageNameNode sn : sns) {
			if (sn == null) {
				continue;
			}
			name = sn.getName();
			if (BrStringUtils.isEmpty(name)) {
				continue;
			}
			id = sn.getId();
			if(snToId.containsKey(id)){
				continue;
			}
			snToId.put(id, name);
		}
		return snToId;
	}
}
