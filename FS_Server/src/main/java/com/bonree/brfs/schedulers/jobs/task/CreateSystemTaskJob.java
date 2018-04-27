package com.bonree.brfs.schedulers.jobs.task;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.curator.RetryPolicy;
import org.apache.curator.RetrySleeper;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryNTimes;
import org.joda.time.DateTime;
import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import org.quartz.UnableToInterruptJobException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bonree.brfs.common.service.Service;
import com.bonree.brfs.common.service.ServiceManager;
import com.bonree.brfs.common.service.impl.DefaultServiceManager;
import com.bonree.brfs.common.task.TaskType;
import com.bonree.brfs.common.utils.BrStringUtils;
import com.bonree.brfs.common.zookeeper.curator.CuratorClient;
import com.bonree.brfs.duplication.storagename.StorageNameManager;
import com.bonree.brfs.duplication.storagename.StorageNameNode;
import com.bonree.brfs.schedulers.ManagerContralFactory;
import com.bonree.brfs.schedulers.task.TasksUtils;
import com.bonree.brfs.schedulers.task.manager.MetaTaskManagerInterface;
import com.bonree.brfs.schedulers.task.manager.impl.ReleaseTaskFactory;
import com.bonree.brfs.schedulers.task.meta.impl.QuartzSimpleInfo;
import com.bonree.brfs.schedulers.task.model.AtomTaskModel;
import com.bonree.brfs.schedulers.task.model.TaskModel;
import com.bonree.brfs.schedulers.task.operation.impl.QuartzOperationStateTask;

public class CreateSystemTaskJob extends QuartzOperationStateTask {
	private static final Logger LOG = LoggerFactory.getLogger("CreateSysTask");
	@Override
	public void caughtException(JobExecutionContext context) {
		// TODO Auto-generated method stub
		LOG.info(" happened Exception !!!");
	}

	@Override
	public void interrupt() throws UnableToInterruptJobException {
		// TODO Auto-generated method stub
		LOG.info(" happened Interrupt !!!");
		
	}

	@Override
	public void operation(JobExecutionContext context) throws Exception {
		ManagerContralFactory mcf = ManagerContralFactory.getInstance();
		
		MetaTaskManagerInterface release = mcf.getTm();
		// 获取开启的任务名称
		List<TaskType> switchList = mcf.getTaskOn();
		if(switchList==null || switchList.isEmpty()){
			throw new NullPointerException("switch on task is empty !!!");
		}
		// 获取可用服务
		String groupName = mcf.getGroupName();
		ServiceManager sm = mcf.getSm();
		// 2.设置可用服务
		List<String> serverIds = getServerIds(sm, groupName);
		if(serverIds == null || serverIds.isEmpty()){
			throw new NullPointerException(" available server list is null");
		}
		// 3.获取storageName
		StorageNameManager snm = mcf.getSnm();
		List<StorageNameNode> snList = snm.getStorageNameNodeList();
		List<AtomTaskModel> snAtomTaskList = new ArrayList<AtomTaskModel>();
		LOG.info("create task success !!!!!");
	}
	
	public List<TaskModel> createTaskModel(List<StorageNameNode> snList,TaskType taskType, long currentTime){
		List<TaskModel> taskList = new ArrayList<TaskModel>();
		long creatTime = 0;
		long ttl = 0;
		for(StorageNameNode snn : snList){
			//TODO:俞朋添加创建时间
			creatTime = snn.getCreateTime();
			ttl = snn.getTtl();
			//系统删除任务判断
			if(TaskType.SYSTEM_DELETE.equals(taskType) && (currentTime - creatTime) < ttl){
				continue;
			}
			//归并任务校验
			//TODO:需要与俞朋的目录结构保持一致
			DateTime date = new DateTime().withMillis(currentTime - ttl);
			
			
			
		}
		return taskList;
	}
	/***
	 * 概述：获取存活的serverid
	 * @param sm
	 * @param groupName
	 * @return
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	private List<String> getServerIds(ServiceManager sm, String groupName){
		List<String> sids = new ArrayList<>();
		List<Service> sList = sm.getServiceListByGroup(groupName);
		if(sList == null || sList.isEmpty()){
			return sids;
		}
		String sid = null;
		for(Service server : sList){
			sid = server.getServiceId();
			if(BrStringUtils.isEmpty(sid)){
				continue;
			}
			sids.add(sid);
		}
		return sids;
	}
	
	

}
