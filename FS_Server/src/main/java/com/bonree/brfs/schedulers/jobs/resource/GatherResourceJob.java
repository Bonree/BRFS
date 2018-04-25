
package com.bonree.brfs.schedulers.jobs.resource;

import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
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
import com.bonree.brfs.resourceschedule.service.impl.AvailableServerFactory;
import com.bonree.brfs.schedulers.ManagerContralFactory;
import com.bonree.brfs.schedulers.jobs.JobDataMapConstract;
import com.bonree.brfs.schedulers.task.operation.impl.QuartzOperationStateTask;

public class GatherResourceJob extends QuartzOperationStateTask {
	private static final Logger LOG = LoggerFactory.getLogger("GATHER");
	private static Queue<StateMetaServerModel> queue = new ConcurrentLinkedQueue<StateMetaServerModel>();

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
	 * @param zkUrl
	 * @param groupName
	 * @param serverId
	 * @param dataDir
	 * @param inverTime
	 * @throws Exception
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	// TODO:此方法中需要添加报警日志
	private void updateResource(String dataDir, long inverTime) throws Exception {

		StorageNameManager snManager = null;
		ServiceManager sManager = null;

		// 0.计算原始状态信息
		List<StatServerModel> lists = GatherResource.calcState(queue);
		if (lists == null || lists.isEmpty()) {
			LOG.warn("server state list is null !!");
			return;
		}
		ManagerContralFactory mcf = ManagerContralFactory.getInstance();
		String groupName = mcf.getGroupName();
		String serverId = mcf.getServerId();

		// 1-1初始化storagename管理器
		// TODO:俞朋 的 获取storageName
		snManager = mcf.getSnm();
		// 1-2.初始化service管理器
		sManager = mcf.getSm();

		// 2.获取storage信息
		List<StorageNameNode> storageNames = snManager.getStorageNameNodeList();
		List<String> storagenameList = getStorageNames(storageNames);
		// 3.计算状态值
		StatServerModel sum = GatherResource.calcStatServerModel(lists, storagenameList, inverTime, dataDir);
		if (sum == null) {
			LOG.warn("calc server state is null !!!");
			return;
		}
		// 4.获取集群基础信息
		List<Service> serverList = sManager.getServiceListByGroup(groupName);
		// 5.计算集群基础基础信息
		BaseMetaServerModel base = calcCluster(serverList);
		if (base == null) {
			LOG.warn("base server info is null !!!");
			return;
		}
		// 6.获取本机信息
		Service server = sManager.getServiceById(groupName, serverId);
		LOG.info("service :{}", JsonUtils.toJsonString(server));
		String content = server.getPayload();
		ServerModel sinfo = checkAndCreateServerModel(content, serverId, dataDir);
		LOG.info("service server :{}",JsonUtils.toJsonString(sinfo));
		// 7.计算Resource值
		ResourceModel resource = GatherResource.calcResourceValue(base, sum);
		if (resource == null) {
			LOG.warn("calc resource value is null !!!");
			return;
		}
		LOG.info("resource : {}", resource);
		sinfo.setResource(resource);
		String result = JsonUtils.toJsonString(sinfo);
		if (BrStringUtils.isEmpty(result)) {
			LOG.warn("resource convern json result is null !!!");
			return;
		}
		sManager.updateService(groupName, serverId, result);
		LOG.info("update zookeeper complete");
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
	private BaseMetaServerModel calcCluster(List<Service> serverList) {
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
			if (BrStringUtils.isEmpty(content)) {
				continue;
			}
			sinfo = JsonUtils.toObject(content, ServerModel.class);
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
}
