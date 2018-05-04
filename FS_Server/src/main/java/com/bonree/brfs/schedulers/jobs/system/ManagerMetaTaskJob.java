
package com.bonree.brfs.schedulers.jobs.system;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import org.quartz.UnableToInterruptJobException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bonree.brfs.common.service.Service;
import com.bonree.brfs.common.service.ServiceManager;
import com.bonree.brfs.common.task.TaskType;
import com.bonree.brfs.common.utils.BrStringUtils;
import com.bonree.brfs.schedulers.ManagerContralFactory;
import com.bonree.brfs.schedulers.jobs.JobDataMapConstract;
import com.bonree.brfs.schedulers.task.TasksUtils;
import com.bonree.brfs.schedulers.task.manager.MetaTaskManagerInterface;
import com.bonree.brfs.schedulers.task.manager.impl.DefaultReleaseTask;
import com.bonree.brfs.schedulers.task.manager.impl.ReleaseTaskFactory;
import com.bonree.brfs.schedulers.task.operation.impl.QuartzOperationStateTask;

public class ManagerMetaTaskJob extends QuartzOperationStateTask {
	private static final Logger LOG = LoggerFactory.getLogger("ManagerMetaTaskJob");
	

	@Override
	public void caughtException(JobExecutionContext context) {
	}

	@Override
	public void interrupt() throws UnableToInterruptJobException {

	}

	@Override
	public void operation(JobExecutionContext context) throws Exception {
		// TODO 确定全局的数据读取key
		JobDataMap data = context.getJobDetail().getJobDataMap();
		// 任务过期时间 ms
		String ttlTimeStr = data.getString(JobDataMapConstract.TASK_EXPIRED_TIME);
		LOG.info("task ttl time : {}", ttlTimeStr);
		long ttlTime = Long.valueOf(ttlTimeStr);
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
		for(TaskType taskType : switchList){
			try {
				release.reviseTaskStat(taskType.name(), ttlTime, serverIds);
			} catch (Exception e) {
				LOG.warn("{}", e.getMessage());
			}
		}
		LOG.info("revise task success !!!");
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
