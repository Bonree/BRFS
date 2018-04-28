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
import com.bonree.brfs.duplication.storagename.StorageNameNode;
import com.bonree.brfs.schedulers.ManagerContralFactory;
import com.bonree.brfs.schedulers.task.TasksUtils;
import com.bonree.brfs.schedulers.task.manager.MetaTaskManagerInterface;
import com.bonree.brfs.schedulers.task.manager.RunnableTaskInterface;
import com.bonree.brfs.schedulers.task.manager.SchedulerManagerInterface;
import com.bonree.brfs.schedulers.task.manager.impl.ReleaseTaskFactory;
import com.bonree.brfs.schedulers.task.meta.SumbitTaskInterface;
import com.bonree.brfs.schedulers.task.meta.impl.QuartzSimpleInfo;
import com.bonree.brfs.schedulers.task.model.AtomTaskModel;
import com.bonree.brfs.schedulers.task.model.TaskExecutablePattern;
import com.bonree.brfs.schedulers.task.model.TaskModel;
import com.bonree.brfs.schedulers.task.model.TaskRunPattern;
import com.bonree.brfs.schedulers.task.operation.impl.QuartzOperationStateTask;

public class OperationTaskJob extends QuartzOperationStateTask {
	private static final Logger LOG = LoggerFactory.getLogger("OperationTaskJob");
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
		JobDataMap data = context.getJobDetail().getJobDataMap();
		ManagerContralFactory mcf = ManagerContralFactory.getInstance();
		MetaTaskManagerInterface release = mcf.getTm();
		if(release == null){
			throw new NullPointerException("MetaTaskManager is empty !!!");
		}
		List<TaskType> switchList = mcf.getTaskOn();
		if(switchList == null || switchList.isEmpty()){
			throw new NullPointerException("MetaTaskManager is empty !!!");
		}
		SchedulerManagerInterface schd = mcf.getStm();
		if(schd == null){
			throw new NullPointerException("SchedulerManagerInterface is empty !!!");
		}
		RunnableTaskInterface runTask = mcf.getRt();
		if(runTask == null){
			throw new NullPointerException("RunnableTaskInterface is empty !!!");
		}
		String typeName = null;
		String prexTaskName = null;
		String currentTaskName = null;
		TaskModel task = null;
		TaskRunPattern runPattern =null;
		int poolSize = 0;
		int sumbitSize = 0;
		SumbitTaskInterface sumbitTask = null;
		for(TaskType taskType : switchList){
			try {
				typeName = taskType.name();
				poolSize = schd.getTaskPoolSize(typeName);
				sumbitSize = schd.getSumbitedTaskCount(typeName);
				//判断任务是否可以执行
				boolean isRun = runTask.taskRunnable(taskType.code(), poolSize, sumbitSize);
				if(!isRun){
					LOG.warn("resource is limit !!! skip {} !!!",typeName);
					continue;
				}
				if(data.containsKey(typeName)){
					prexTaskName = data.getString(typeName);
				}
				if(BrStringUtils.isEmpty(prexTaskName)){
					prexTaskName = release.getFirstTaskName(typeName);
					currentTaskName = prexTaskName;
				}else{
					currentTaskName = release.getNextTaskName(typeName, prexTaskName);
				}
				if(BrStringUtils.isEmpty(currentTaskName)){
					LOG.info("taskType :{} queue is empty ,skiping !!!",typeName);
					continue;
				}
				//获取任务信息
				task = release.getTaskContentNodeInfo(typeName, currentTaskName);
				if(task == null){
					LOG.warn("taskType :{} taskName: {} is vaild ,skiping !!!",typeName, currentTaskName);
					data.put(typeName, currentTaskName);
					continue;
				}
				// 获取执行策略
				runPattern = runTask.taskRunnPattern(task);
				if(runPattern == null){
					LOG.warn("TaskRunPattern is null will do it once");
					runPattern = new TaskRunPattern();
					runPattern.setRepeateCount(1);
					runPattern.setSleepTime(1000);
				}
				
				//创建任务提交信息
				sumbitTask = null;
				
				boolean isSumbit = schd.addTask(typeName, sumbitTask);
				LOG.info("sumbit type:{}, taskName :{}, state:{}", typeName, currentTaskName, isSumbit);
				if(!isSumbit){
					LOG.info("next cycle will sumbit against type : {}, taskName : {}", typeName, currentTaskName);
					continue;
				}
				
				//更新任务执行的位置
				data.put(typeName, currentTaskName);
			}
			catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
	}
	
	private SumbitTaskInterface createSimpleTask(TaskModel taskModel, TaskRunPattern runPattern, String taskName, String serverId){
		QuartzSimpleInfo task = new QuartzSimpleInfo();
		task.setRunNowFlag(true);
		task.setCycleFlag(false);
		task.setTaskName(taskName);
//		task.set
		return task;
	}
	
	

}
