package com.bonree.brfs.schedulers.jobs.resource;

import java.util.ArrayList;
import java.util.List;

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
import com.bonree.brfs.resourceschedule.service.AvailableServerInterface;
import com.bonree.brfs.resourceschedule.service.impl.AvailableServerFactory;
import com.bonree.brfs.schedulers.ManagerContralFactory;
import com.bonree.brfs.schedulers.jobs.JobDataMapConstract;
import com.bonree.brfs.schedulers.task.operation.impl.QuartzOperationStateTask;
/*****************************************************************************
 * 版权信息：北京博睿宏远数据科技股份有限公司
 * Copyright: Copyright (c) 2007北京博睿宏远数据科技股份有限公司,Inc.All Rights Reserved.
 * 
 * @date 2018年4月24日 下午4:36:34
 * @Author: <a href=mailto:zhucg@bonree.com>朱成岗</a>
 * @Description: 将ReSource任务同步
 *****************************************************************************
 */
public class AsynJob extends QuartzOperationStateTask {
	private static final Logger LOG = LoggerFactory.getLogger("AsynJob");
	private static final String INVERAL_TIME = "INVERAL_TIME";
	@Override
	public void caughtException(JobExecutionContext context) {
		// TODO Auto-generated method stub
		LOG.info("Exception : {}",context.get("ExceptionMessage"));
	}

	@Override
	public void interrupt() throws UnableToInterruptJobException {
		// TODO Auto-generated method stub
		LOG.info("Interrupt job :", this.getClass().getName());
	}

	@Override
	public void operation(JobExecutionContext context) throws Exception {
		JobDataMap data = context.getJobDetail().getJobDataMap();
		if(data == null || data.isEmpty()){
			throw new NullPointerException("job data map is empty");
		}
		// 1.获取当前时间
		long currentTime = System.currentTimeMillis();
		// 2.获取更新间隔
		if(!data.containsKey(INVERAL_TIME)){
			long gatherInveral = data.getLong(JobDataMapConstract.GATHER_INVERAL_TIME);
			int calcCount = data.getInt(JobDataMapConstract.CALC_RESOURCE_COUNT);
			data.put(INVERAL_TIME, gatherInveral * (calcCount-1) +"");
		}
		long inveralTime = data.getLong(INVERAL_TIME);
		
		ManagerContralFactory mcf = ManagerContralFactory.getInstance();
		// 3.获取可用服务接口
		AvailableServerInterface avaliable = mcf.getAsm();
		// 4.间隔时间不足的跳出
		if((currentTime - avaliable.getLastUpdateTime()) < inveralTime){
			LOG.info("skip update server resource !!! time not enough");
			return;
		}
		// 5.从zk获取resource资源
		List<ResourceModel> datas = getResourceList(mcf);
		if(datas == null || datas.isEmpty()){
			LOG.info("skip update server resource !!! resource is empty");
			return;
		}
		// 6.更新可用接口信息
		avaliable.update(datas);
		LOG.info("update Interface server resource  complete!!!");
	}
	/**
	 * 概述：获取resourceValue
	 * @param zookeeperAddress
	 * @param clusterName
	 * @return
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	private static List<ResourceModel> getResourceList(ManagerContralFactory mcf){
		List<ResourceModel> resources = new ArrayList<ResourceModel>();
		ServiceManager sManager = null;
		try {
			String clusterName = mcf.getGroupName();
			sManager = mcf.getSm();
			List<Service> serverList = sManager.getServiceListByGroup(clusterName);
			if (serverList == null || serverList.isEmpty()) {
				LOG.warn("service info is empty !!!");
				return resources;
			}
			return getResources(serverList);
			
		}
		catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return resources;
	}
	/**
	 * 概述：计算集群基础信息
	 * @param serverList
	 * @return
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	private static List<ResourceModel> getResources(List<Service> serverList) {
		if (serverList == null || serverList.isEmpty()) {
			return null;
		}
		// 2.获取集群基础基础信息
		List<ResourceModel> resources = new ArrayList<ResourceModel>();
		ResourceModel resource = null;
		ServerModel sinfo = null;
		String content = null;
		for (Service server : serverList) {
			content = server.getPayload();
			// 2-1.过滤掉为空的
			if (BrStringUtils.isEmpty(content)) {
				continue;
			}
			sinfo = getServerModel(server);
			// 2-2 过滤为null的
			if (sinfo == null) {
				continue;
			}
			resource = sinfo.getResource();
			// 2-3 过滤base为null的
			if (resource == null) {
				continue;
			}
			resources.add(resource);
		}
		if (resources.isEmpty()) {
			return null;
		}
		return resources;
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
}
