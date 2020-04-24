
package com.bonree.brfs.schedulers.jobs.resource;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

import com.bonree.brfs.identification.impl.DiskDaemon;
import com.bonree.brfs.partition.model.LocalPartitionInfo;
import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bonree.brfs.common.utils.BrStringUtils;
import com.bonree.brfs.common.utils.JsonUtils;
import com.bonree.brfs.common.utils.JsonUtils.JsonException;
import com.bonree.brfs.common.zookeeper.ZookeeperClient;
import com.bonree.brfs.common.zookeeper.curator.CuratorClient;
import com.bonree.brfs.configuration.Configs;
import com.bonree.brfs.configuration.units.ResourceConfigs;
import com.bonree.brfs.duplication.storageregion.StorageRegion;
import com.bonree.brfs.duplication.storageregion.StorageRegionManager;
import com.bonree.brfs.email.EmailPool;
import com.bonree.brfs.resourceschedule.commons.GatherResource;
import com.bonree.brfs.resourceschedule.model.BaseMetaServerModel;
import com.bonree.brfs.resourceschedule.model.LimitServerResource;
import com.bonree.brfs.resourceschedule.model.ResourceModel;
import com.bonree.brfs.resourceschedule.model.StatServerModel;
import com.bonree.brfs.resourceschedule.model.StateMetaServerModel;
import com.bonree.brfs.schedulers.ManagerContralFactory;
import com.bonree.brfs.schedulers.task.manager.RunnableTaskInterface;
import com.bonree.brfs.schedulers.task.operation.impl.QuartzOperationStateTask;
import com.bonree.brfs.schedulers.utils.JobDataMapConstract;
import com.bonree.mail.worker.MailWorker;
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
	private static final Logger LOG = LoggerFactory.getLogger(GatherResourceJob.class);
	private static Queue<StateMetaServerModel> queue = new ConcurrentLinkedQueue<StateMetaServerModel>();
	private static long preTime = 0L;
	private static long INVERTTIME = Configs.getConfiguration().getConfig(ResourceConfigs.CONFIG_RESOURCE_EMAIL_INVERT)*1000;

	@Override
	public void interrupt(){
	}

	@Override
	public void operation(JobExecutionContext context) throws Exception {
		JobDataMap data = context.getJobDetail().getJobDataMap();
		if (data == null || data.isEmpty()) {
			throw new NullPointerException("job data map is empty");
		}
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
		Collection<String> dataDir = getDataDirs(mcf.getDaemon());
		if(!client.checkExists(bPath)) {
			saveLocal(client, dataDir, bPath);
		}
		long gatherInveral = data.getLongValueFromString(JobDataMapConstract.GATHER_INVERAL_TIME);
		int count = data.getIntFromString(JobDataMapConstract.CALC_RESOURCE_COUNT);
		StateMetaServerModel metaSource = GatherResource.gatherResource(ip,dataDir);
		if (metaSource != null) {
			queue.add(metaSource);
			LOG.info("gather stat info !!! {}", queue.size());
			
		}
		int queueSize = queue.size();
		if (queueSize < count) {
			return ;
		}
		// 更新任务的可执行资源
		StatServerModel sum = calcStateServer(gatherInveral, count);
		if (sum != null) {
			RunnableTaskInterface rt = mcf.getRt();
			rt.update(sum);
		}
		// 计算可用服务
		BaseMetaServerModel base = getClusterBases(client, basePath+"/base", mcf.getGroupName());
		if (base == null) {
			return;
		}
		String serverId = mcf.getServerId();
		// 计算资源值
		ResourceModel resource = GatherResource.calcResourceValue(base, sum,serverId,ip);
		if (resource == null) {
			LOG.warn("calc resource value is null !!!");
			return;
		}
		sendWarnEmail(resource,mcf.getLimitServerResource());
        resource.setServerId(serverId);
        Map<Integer, String> snIds = getStorageNameIdWithName();
        resource.setSnIds(snIds);
		byte[] rdata= JsonUtils.toJsonBytesQuietly(resource);
		String rPath = basePath+"/resource/"+serverId;
		if(!saveDataToZK(client, rPath, rdata)) {
			LOG.error("resource content :{} save to zk fail !!!",JsonUtils.toJsonStringQuietly(resource));
		}else {
			LOG.info("resource: succefull !!!");
		}
		
		BaseMetaServerModel local = GatherResource.gatherBase(dataDir);
		if(local == null) {
			LOG.error("gather base data is empty !!!");
			return;
		}
		byte[] bData = JsonUtils.toJsonBytesQuietly(local);
		if(!saveDataToZK(client, bPath, bData)) {
			LOG.error("base content : {} save to zk fail!!!",JsonUtils.toJsonStringQuietly(local));
		}
		saveLocal(client,dataDir, bPath);
		
	}
	public void sendWarnEmail(ResourceModel resource, LimitServerResource limit){
		Map<String,Long> remainSize = resource.getLocalRemainSizeValue();
		String mountPoint;
		long remainsize;
		Map<String,String> map = new HashMap<>();
		for(Map.Entry<String,Long> entry : remainSize.entrySet()){
			mountPoint = entry.getKey();
			remainsize = entry.getValue();
			if(remainsize < limit.getRemainForceSize()){
				map.put(mountPoint,"磁盘剩余量低于限制值 "+ limit.getRemainForceSize()+", 当前剩余值为"+remainsize+" 服务即将参与拒绝写入服务");
			}else if(remainsize < limit.getRemainWarnSize()){
				map.put(mountPoint,"磁盘剩余量低于警告值 "+ limit.getRemainWarnSize()+", 当前剩余值为"+remainsize);
			}
		}
		long currentTime = System.currentTimeMillis();
		if(!map.isEmpty()&& (currentTime - preTime) > INVERTTIME){
			EmailPool emailPool = EmailPool.getInstance();
			MailWorker.Builder builder = MailWorker.newBuilder(emailPool.getProgramInfo());
			builder.setModel(this.getClass().getSimpleName()+"模块服务告警");
			builder.setMessage(resource.getServerId()+"("+resource.getHost()+") 磁盘资源即将不足");
			builder.setVariable(map);
			emailPool.sendEmail(builder);
			preTime = currentTime;
		}

	}
	public Collection<String> getDataDirs(DiskDaemon diskDaemon){
		Collection<LocalPartitionInfo> partitions = diskDaemon.getPartitions();
		Collection<String> dirs = new HashSet<>();
		for(LocalPartitionInfo local : partitions){
			dirs.add(local.getDataDir());
		}
		return dirs;
	}
	public void saveLocal(CuratorClient client,Collection<String> dataDirs,String bPath) {
		BaseMetaServerModel local = GatherResource.gatherBase( dataDirs);
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
			LOG.error("save resource to zk {}", e);
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
		List<BaseMetaServerModel> bases = new ArrayList<>();
		String cPath;
		byte[] data;
		BaseMetaServerModel tmp;
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
				LOG.error("get base from zk error {}", e);
			}
		}
		// 2.计算集群基础基础信息
		BaseMetaServerModel base = GatherResource.collectBaseMetaServer(bases);
		return base;
	}

	/**
	 * 概述：计算队列的状态信息
	 * @param inverTime
	 * @return
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	private StatServerModel calcStateServer(long inverTime, int count) {
		StatServerModel sum = null;
		// 0.计算原始状态信息
		List<StatServerModel> lists = GatherResource.calcState(queue,count);
		if (lists == null || lists.isEmpty()) {
			LOG.warn("server state list is null !!");
			return sum;
		}
		ManagerContralFactory mcf = ManagerContralFactory.getInstance();

		// 1-1初始化storagename管理器
		StorageRegionManager snManager = mcf.getSnm();
		// 2.获取storage信息
		List<StorageRegion> storageNames = snManager.getStorageRegionList();
		List<String> storagenameList = getStorageNames(storageNames);
		// 3.计算状态值
		sum = GatherResource.calcStatServerModel(lists,  inverTime);
		return sum;
	}

	/***
	 * 概述：获取storageName的名称
	 * @param storageNames
	 * @return
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	private List<String> getStorageNames(List<StorageRegion> storageNames) {
		List<String> snList = new ArrayList<>();

		if (storageNames == null || storageNames.isEmpty()) {
			return snList;
		}
		String tmp;
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
	 * 概述：获取storageName关系
	 * @return
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	private Map<Integer,String> getStorageNameIdWithName(){
		ManagerContralFactory mcf = ManagerContralFactory.getInstance();
		StorageRegionManager snm = mcf.getSnm();
		List<StorageRegion> sns = snm.getStorageRegionList();
		Map<Integer,String> snToId = new ConcurrentHashMap<>();
		if(sns == null || sns.isEmpty()){
			return snToId;
		}
		int id;
		String name;
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
