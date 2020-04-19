package com.bonree.brfs.schedulers.jobs.system;

import java.util.List;
import java.util.Map;

import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bonree.brfs.common.task.TaskType;
import com.bonree.brfs.common.utils.JsonUtils.JsonException;
import com.bonree.brfs.email.EmailPool;
import com.bonree.brfs.common.utils.Pair;
import com.bonree.brfs.schedulers.ManagerContralFactory;
import com.bonree.brfs.schedulers.jobs.biz.SystemCheckJob;
import com.bonree.brfs.schedulers.jobs.biz.SystemDeleteJob;
import com.bonree.brfs.schedulers.jobs.biz.UserDeleteJob;
import com.bonree.brfs.schedulers.task.manager.MetaTaskManagerInterface;
import com.bonree.brfs.schedulers.task.manager.RunnableTaskInterface;
import com.bonree.brfs.schedulers.task.manager.SchedulerManagerInterface;
import com.bonree.brfs.schedulers.task.meta.SumbitTaskInterface;
import com.bonree.brfs.schedulers.task.meta.impl.QuartzSimpleInfo;
import com.bonree.brfs.schedulers.task.model.TaskModel;
import com.bonree.brfs.schedulers.task.model.TaskRunPattern;
import com.bonree.brfs.schedulers.task.operation.impl.QuartzOperationStateTask;
import com.bonree.brfs.schedulers.utils.JobDataMapConstract;
import com.bonree.brfs.schedulers.utils.TaskStateLifeContral;
import com.bonree.mail.worker.MailWorker;

public class OperationTaskJob extends QuartzOperationStateTask {
	private static final Logger LOG = LoggerFactory.getLogger(OperationTaskJob.class);

	@Override
	public void interrupt(){
	}

	@Override
	public void operation(JobExecutionContext context){
		JobDataMap data = context.getJobDetail().getJobDataMap();
		ManagerContralFactory mcf = ManagerContralFactory.getInstance();
		MetaTaskManagerInterface release = mcf.getTm();
		if(release == null){
			throw new NullPointerException("MetaTaskManager is empty !!!");
		}
		List<TaskType> switchList = mcf.getTaskOn();
		if(switchList == null || switchList.isEmpty()){
			LOG.warn("switch task is empty !!!");
			return;
		}
		SchedulerManagerInterface schd = mcf.getStm();
		if(schd == null){
			throw new NullPointerException("SchedulerManagerInterface is empty !!!");
		}
		RunnableTaskInterface runTask = mcf.getRt();
		if(runTask == null){
			throw new NullPointerException("RunnableTaskInterface is empty !!!");
		}
		String typeName;
		String currentTaskName;
		TaskModel task;
		TaskRunPattern runPattern;
		int poolSize;
		int sumbitSize;
		String serverId = mcf.getServerId();
		SumbitTaskInterface sumbitTask;
		//判断是否有恢复任务，有恢复任务则不进行创建
		boolean rebalanceFlag = mcf.getTaskMonitor().isExecute();
		for(TaskType taskType : switchList){
			sumbitTask = null;
			try {
				if(TaskType.SYSTEM_COPY_CHECK.equals(taskType)){
					continue;
				}
				typeName = taskType.name();
				poolSize = schd.getTaskPoolSize(typeName);
				sumbitSize = schd.getSumbitedTaskCount(typeName);
				//判断任务是否可以执行
				boolean isRun = runTask.taskRunnable(taskType.code(), poolSize, sumbitSize);
				if(!isRun){
					LOG.warn("resource is limit !!! skip {} !!!",typeName);
					continue;
				}
				int retryCount = 3;
				if(TaskType.SYSTEM_CHECK.equals(taskType)||TaskType.SYSTEM_MERGER.equals(taskType)|| TaskType.SYSTEM_DELETE.equals(taskType)){
					retryCount = 0;
				}
				Pair<String,TaskModel> taskPair = TaskStateLifeContral.getCurrentOperationTask(release, typeName, serverId, retryCount);
				if(taskPair == null){
					LOG.warn("taskType :{} taskName: null is vaild ,skiping !!!",typeName);
					continue;
				}
				currentTaskName = taskPair.getFirst();

				task = TaskStateLifeContral.changeRunTaskModel(taskPair.getSecond(), mcf.getDaemon());
				// 获取执行策略
				runPattern = runTask.taskRunnPattern(task);
				if(runPattern == null){
					LOG.warn("TaskRunPattern is null will do it once");
					runPattern = new TaskRunPattern();
					runPattern.setRepeateCount(1);
					runPattern.setSleepTime(1000);
				}
				// 创建任务提交信息
				if(TaskType.SYSTEM_DELETE.equals(taskType)){
					sumbitTask = createSimpleTask(task, runPattern, currentTaskName, mcf.getServerId(), SystemDeleteJob.class.getCanonicalName());
				}
				if(TaskType.SYSTEM_CHECK.equals(taskType)){
					sumbitTask = createSimpleTask(task, runPattern, currentTaskName, mcf.getServerId(), SystemCheckJob.class.getCanonicalName());
				}
				if(TaskType.USER_DELETE.equals(taskType)){
					sumbitTask = createSimpleTask(task, runPattern, currentTaskName, mcf.getServerId(), UserDeleteJob.class.getCanonicalName());
				}
				if(rebalanceFlag && TaskType.SYSTEM_CHECK.equals(taskType)) {
					LOG.warn("rebalance task running !! Skip {} sumbit",taskType.name());
					continue;
				}
				if(sumbitTask == null) {
					LOG.warn("sumbit type:{}, taskName :{}, taskcontent is null", typeName, currentTaskName);
					continue;
				}
				boolean isSumbit = schd.addTask(typeName, sumbitTask);
				LOG.info("sumbit type:{}, taskName :{}, state:{}", typeName, currentTaskName, isSumbit);
				if(!isSumbit){
					LOG.warn("next cycle will sumbit against type : {}, taskName : {}", typeName, currentTaskName);
					continue;
				}
				// 更新任务状态
				//更新任务执行的位置
				data.put(typeName, currentTaskName);
			}
			catch (Exception e) {
				LOG.error("{}",e);
				EmailPool emailPool = EmailPool.getInstance();
				MailWorker.Builder builder = MailWorker.newBuilder(emailPool.getProgramInfo());
				builder.setModel(this.getClass().getSimpleName()+"模块服务发生问题");
				builder.setException(e);
				builder.setMessage("执行任务发生错误");
				builder.setVariable(data.getWrappedMap());
				emailPool.sendEmail(builder);
			}
		}
	}


	/**
	 * 概述：生成任务信息
	 * @param taskModel
	 * @param runPattern
	 * @param taskName
	 * @param serverId
	 * @param clazzName
	 * @return
	 * @throws JsonException
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	private SumbitTaskInterface createSimpleTask(TaskModel taskModel, TaskRunPattern runPattern, String taskName, String serverId,String clazzName) throws Exception{
		QuartzSimpleInfo task = new QuartzSimpleInfo();
		task.setRunNowFlag(true);
		task.setCycleFlag(false);
		task.setTaskName(taskName);
		task.setTaskGroupName(TaskType.valueOf(taskModel.getTaskType()).name());
		task.setRepeateCount(runPattern.getRepeateCount());
		task.setInterval(runPattern.getSleepTime());
		Map<String,String> dataMap = JobDataMapConstract.createOperationDataMap(taskName,serverId, taskModel, runPattern);
		if(dataMap != null && !dataMap.isEmpty()){
			task.setTaskContent(dataMap);
		}
		task.setClassInstanceName(clazzName);
		return task;
	}



}
