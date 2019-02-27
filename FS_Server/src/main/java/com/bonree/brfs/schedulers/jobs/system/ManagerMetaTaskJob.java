
package com.bonree.brfs.schedulers.jobs.system;

import java.util.ArrayList;
import java.util.List;

import com.bonree.brfs.email.EmailPool;
import com.bonree.mail.worker.MailWorker;
import com.bonree.mail.worker.ProgramInfo;
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
import com.bonree.brfs.schedulers.task.manager.MetaTaskManagerInterface;
import com.bonree.brfs.schedulers.task.model.TaskTypeModel;
import com.bonree.brfs.schedulers.task.operation.impl.QuartzOperationStateTask;
import com.bonree.brfs.schedulers.utils.JobDataMapConstract;

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
		LOG.info("----------> revise task work");
		JobDataMap data = context.getJobDetail().getJobDataMap();
		// 任务过期时间 ms
		String ttlTimeStr = data.getString(JobDataMapConstract.TASK_EXPIRED_TIME);
		LOG.info("task ttl time : {}", ttlTimeStr);
		long ttlTime = Long.parseLong(ttlTimeStr);
		ManagerContralFactory mcf = ManagerContralFactory.getInstance();
		
		MetaTaskManagerInterface release = mcf.getTm();
		// 获取可用服务
		String groupName = mcf.getGroupName();
		ServiceManager sm = mcf.getSm();
		// 2.设置可用服务
		List<String> serverIds = getServerIds(sm, groupName);
		if(serverIds == null || serverIds.isEmpty()){
			LOG.warn("available server list is null");
			return;
		}
		for(TaskType taskType : TaskType.values()){
			try {
				release.reviseTaskStat(taskType.name(), ttlTime, serverIds);
			} catch (Exception e) {
				LOG.warn("{}", e.getMessage());
				MailWorker.Builder builder = MailWorker.newBuilder(ProgramInfo.getInstance());
				builder.setModel(this.getClass().getSimpleName()+"模块服务发生问题");
				builder.setException(e);
				builder.setMessage("管理任务数据发生错误");
				builder.setVariable(data.getWrappedMap());
				EmailPool.getInstance().sendEmail(builder);
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
