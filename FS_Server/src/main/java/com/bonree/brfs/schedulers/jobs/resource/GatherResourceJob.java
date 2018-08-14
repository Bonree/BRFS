
package com.bonree.brfs.schedulers.jobs.resource;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import org.quartz.UnableToInterruptJobException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bonree.brfs.common.service.ServiceManager;
import com.bonree.brfs.common.utils.BrStringUtils;
import com.bonree.brfs.common.utils.JsonUtils;
import com.bonree.brfs.common.utils.JsonUtils.JsonException;
import com.bonree.brfs.common.zookeeper.ZookeeperClient;
import com.bonree.brfs.common.zookeeper.curator.CuratorClient;
import com.bonree.brfs.duplication.storageregion.StorageRegion;
import com.bonree.brfs.duplication.storageregion.StorageRegionManager;
import com.bonree.brfs.resourceschedule.commons.GatherResource;
import com.bonree.brfs.resourceschedule.model.BaseMetaServerModel;
import com.bonree.brfs.resourceschedule.model.ResourceModel;
import com.bonree.brfs.resourceschedule.model.StatServerModel;
import com.bonree.brfs.resourceschedule.model.StateMetaServerModel;
import com.bonree.brfs.schedulers.ManagerContralFactory;
import com.bonree.brfs.schedulers.task.manager.RunnableTaskInterface;
import com.bonree.brfs.schedulers.task.operation.impl.QuartzOperationStateTask;
import com.bonree.brfs.schedulers.utils.JobDataMapConstract;
/*****************************************************************************
 * 版权信息：北京博睿宏远数据科技股份有限公司
 * Copyright: Copyright (c) 2007北京博睿宏远数据科技股份有限公司,Inc.All Rights Reserved.
 * 
 * @date 2018年7月24日 上午11:08:39
 * @Author: <a href=mailto:zhucg@bonree.com>朱成岗</a>
 * @Description:资源采集模块
 *****************************************************************************
 */
public class GatherResourceJob extends QuartzOperationStateTask {
	private static final Logger LOG = LoggerFactory.getLogger("GATHER");
	private static Queue<StateMetaServerModel> queue = new ConcurrentLinkedQueue<StateMetaServerModel>();
	private static StateMetaServerModel prexState = null;
//	private static ZookeeperClient client = null;

	@Override
	public void caughtException(JobExecutionContext context) {
	}

	@Override
	public void interrupt() throws UnableToInterruptJobException {
	}

	@Override
	public void operation(JobExecutionContext context) throws Exception {
		JobDataMap data = context.getJobDetail().getJobDataMap();
		if (data == null || data.isEmpty()) {
			throw new NullPointerException("job data map is empty");
		}
		String dataDir = data.getString(JobDataMapConstract.DATA_PATH);
		String zkPath = data.getString(JobDataMapConstract.BASE_SERVER_ID_PATH);
		String ip = data.getString(JobDataMapConstract.IP);
		ManagerContralFactory mcf = ManagerContralFactory.getInstance();
		CuratorClient client = mcf.getClient();
		if(client ==null) {
			LOG.error("zookeeper is empty !!!!");
			return;
		}
		String basePath = zkPath+"/"+mcf.getGroupName();
		String bPath = basePath+"/base/"+mcf.getServerId();
		if(!client.checkExists(bPath)) {
			saveLocal(client, mcf.getServerId(), dataDir, bPath);
		}
		long gatherInveral = data.getLongValueFromString(JobDataMapConstract.GATHER_INVERAL_TIME);
		int count = data.getIntFromString(JobDataMapConstract.CALC_RESOURCE_COUNT);
		StateMetaServerModel metaSource = GatherResource.gatherResource(dataDir, ip);
		if (metaSource != null) {
			queue.add(metaSource);
			LOG.info("gather stat info !!! {}", queue.size());
			
		}
		int queueSize = queue.size();
		if (queueSize < count) {
			return ;
		}
		// 更新任务的可执行资源
		StatServerModel sum = calcStateServer(gatherInveral, dataDir);
		if (sum != null) {
			RunnableTaskInterface rt = mcf.getRt();
			rt.update(sum);
		}
		// 计算可用服务
		BaseMetaServerModel base = getClusterBases(client, basePath+"/base", mcf.getGroupName());
		if (base == null) {
			return;
		}
		// 计算资源值
		ResourceModel resource = GatherResource.calcResourceValue(base, sum);
		if (resource == null) {
			LOG.warn("calc resource value is null !!!");
			return;
		}
		Map<Integer, String> snIds = getStorageNameIdWithName();
		resource.setSnIds(snIds);
		resource.setServerId(mcf.getServerId());
		byte[] rdata= JsonUtils.toJsonBytesQuietly(resource);
		String rPath = basePath+"/resource/"+mcf.getServerId();;
		if(!saveDataToZK(client, rPath, rdata)) {
			LOG.error("resource content :{} save to zk fail !!!",JsonUtils.toJsonStringQuietly(resource));
		}else {
			LOG.info("RESOURCE: succefull !!!");
		}
		
		BaseMetaServerModel local = GatherResource.gatherBase(mcf.getServerId(), dataDir);
		if(local == null) {
			LOG.error("gather base data is empty !!!");
			return;
		}
		byte[] bData = JsonUtils.toJsonBytesQuietly(local);
		if(!saveDataToZK(client, bPath, bData)) {
			LOG.error("base content : {} save to zk fail!!!",JsonUtils.toJsonStringQuietly(local));
		}
		saveLocal(client,mcf.getServerId(), dataDir, bPath);
		
	}
	public void saveLocal(CuratorClient client,String serverId, String dataDir,String bPath) {
		BaseMetaServerModel local = GatherResource.gatherBase(serverId, dataDir);
		if(local == null) {
			LOG.error("gather base data is empty !!!");
			return;
		}
		byte[] bData = JsonUtils.toJsonBytesQuietly(local);
		if(!saveDataToZK(client, bPath, bData)) {
			LOG.error("base content : {} save to zk fail!!!",JsonUtils.toJsonStringQuietly(local));
		}
	}
	
	public static  boolean saveDataToZK(ZookeeperClient client, String path, byte[] data) {
		if(data == null|| data.length == 0) {
			LOG.error("save data to zk is empty !!! path :{}", path);
			return false;
		}
		
		try {
			if(client.checkExists(path)) {
				client.setData(path, data);
			}else {
				client.createEphemeral(path, true,data);
			}
		}
		catch (Exception e) {
			LOG.error("{}", e);
			return false;
		}
		return true;
	}
	/**
	 * 概述：获取基本信息
	 * @return
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	private BaseMetaServerModel getClusterBases(ZookeeperClient client, String basePath, String groupName) {
		List<String> childs = client.getChildren(basePath);
		if (childs == null) {
			return null;
		}
		List<BaseMetaServerModel> bases = new ArrayList<BaseMetaServerModel>();
		String cPath = null;
		byte[] data = null;
		BaseMetaServerModel tmp = null;
		for (String child : childs) {
			try {
				cPath = basePath + "/" + child;
				data = client.getData(cPath);
				if (data == null || data.length == 0) {
					continue;
				}
				tmp = JsonUtils.toObject(data, BaseMetaServerModel.class);
				bases.add(tmp);
			}
			catch (JsonException e) {
				LOG.error("{}", e);
			}
		}
		// 2.计算集群基础基础信息
		BaseMetaServerModel base = GatherResource.collectBaseMetaServer(bases);
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
		StorageRegionManager snManager = mcf.getSnm();
		// 1-2.初始化service管理器
		ServiceManager sManager = mcf.getSm();

		// 2.获取storage信息
		List<StorageRegion> storageNames = snManager.getStorageRegionList();
		List<String> storagenameList = getStorageNames(storageNames);
		// 3.计算状态值
		sum = GatherResource.calcStatServerModel(lists, storagenameList, inverTime, dataDir);
		return sum;
	}

//	/**
//	 * 概述：检查并创建服务信息
//	 * @param content
//	 * @param serverId
//	 * @param dataDir
//	 * @return
//	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
//	 */
//	private ServerModel checkAndCreateServerModel(String content, String serverId, String dataDir) {
//		ServerModel sinfo = null;
//		if (BrStringUtils.isEmpty(content)) {
//			sinfo = new ServerModel();
//		}
//		sinfo = JsonUtils.toObjectQuietly(content, ServerModel.class);
//		if (sinfo == null) {
//			sinfo = new ServerModel();
//		}
//		BaseMetaServerModel tmpbase = GatherResource.gatherBase(serverId, dataDir);
//		sinfo.setBase(tmpbase);
//		return sinfo;
//	}

	/***
	 * 概述：获取storageName的名称
	 * @param storageNames
	 * @return
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	private List<String> getStorageNames(List<StorageRegion> storageNames) {
		List<String> snList = new ArrayList<String>();

		if (storageNames == null || storageNames.isEmpty()) {
			return snList;
		}
		String tmp = null;
		for (StorageRegion sn : storageNames) {
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
//	private BaseMetaServerModel calcBaseCluster(List<BaseMetaServerModel> bases) {
//		// 2.获取集群基础基础信息
//		BaseMetaServerModel base = null;
//		ServerModel sinfo = null;
//		String content = null;
//		for (Service server : serverList) {
//			content = server.getPayload();
//			// 2-1.过滤掉为空的
//			sinfo = getServerModel(server);
//			// 2-2 过滤为null的
//			if (sinfo == null) {
//				continue;
//			}
//			base = sinfo.getBase();
//			// 2-3 过滤base为null的
//			if (base == null) {
//				continue;
//			}
//			bases.add(base);
//		}
//		if (bases.isEmpty()) {
//			return null;
//		}
//		return GatherResource.collectBaseMetaServer(bases);
//	}
	/**
	 * 概述：获取storageName关系
	 * @param sns
	 * @return
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	private Map<Integer,String> getStorageNameIdWithName(){
		ManagerContralFactory mcf = ManagerContralFactory.getInstance();
		StorageRegionManager snm = mcf.getSnm();
		List<StorageRegion> sns = snm.getStorageRegionList();
		Map<Integer,String> snToId = new ConcurrentHashMap<Integer,String>();
		if(sns == null || sns.isEmpty()){
			return snToId;
		}
		int id = -1;
		String name = null;
		for (StorageRegion sn : sns) {
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
