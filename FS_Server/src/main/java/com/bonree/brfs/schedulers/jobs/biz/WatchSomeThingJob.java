package com.bonree.brfs.schedulers.jobs.biz;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import org.quartz.UnableToInterruptJobException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bonree.brfs.common.ZookeeperPaths;
import com.bonree.brfs.common.rebalance.Constants;
import com.bonree.brfs.common.zookeeper.curator.CuratorClient;
import com.bonree.brfs.schedulers.ManagerContralFactory;
import com.bonree.brfs.schedulers.task.operation.impl.QuartzOperationStateTask;
import com.bonree.brfs.schedulers.utils.JobDataMapConstract;

public class WatchSomeThingJob extends QuartzOperationStateTask {
	private static final Logger LOG = LoggerFactory.getLogger("WatchSomeThingJob");
	private static Map<Integer,Boolean> StateMap = new ConcurrentHashMap<Integer, Boolean>();
	public static int RECOVERY_STATUSE = 1;
	private static CuratorClient curatorClient =null;
	private static String basePath = null;
	@Override
	public void caughtException(JobExecutionContext context) {
		LOG.info("watch task error !!!");
	}

	@Override
	public void interrupt() throws UnableToInterruptJobException {

	}

	@Override
	public void operation(JobExecutionContext context) throws Exception {
		JobDataMap data = context.getJobDetail().getJobDataMap();
		ManagerContralFactory mcf = ManagerContralFactory.getInstance();
		String zkHost = data.getString(JobDataMapConstract.ZOOKEEPER_ADDRESS);
		String groupName = mcf.getGroupName();
		//获取client
		
		try {
			if(curatorClient == null){
				curatorClient = CuratorClient.getClientInstance(zkHost);
			}
			if(basePath == null){
				//获取监听的目录
				ZookeeperPaths zkPaths = mcf.getZkPath();
				basePath = zkPaths.getBaseRebalancePath();
			}
			String tasksPath=basePath + Constants.SEPARATOR+Constants.TASKS_NODE;
			boolean isIt = isRecovery(curatorClient, tasksPath);
			// 更新map的值
			this.StateMap.put(RECOVERY_STATUSE, isIt);
			//发生副本迁移就删除数据
			if(isIt) {
				WatchDog.abandonFoods();
			}
		}catch (Exception e) {
			LOG.error("{}",e);
		}
	}
	/**
	 * 概述：恢复任务是否执行判断
	 * @param client
	 * @param path
	 * @return
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	private boolean isRecovery(CuratorClient client, String path){
		if(!client.checkExists(path)){
			return false;
		}
		List<String> paths = client.getChildren(path);
		if(paths == null || paths.isEmpty()){
			return false;
		}
		boolean isRun = false;
		String snPath = null;
		List<String> cList = null;
		String tmpPath = null;
		byte[]data = null;
		for(String sn : paths){
			snPath = path + Constants.SEPARATOR +sn;
			cList = client.getChildren(snPath);
			if(cList !=null && !cList.isEmpty()){
				//TODO 防御日志
				tmpPath = snPath +"/"+ cList.get(0);
				data = client.getData(tmpPath);
				LOG.info("path : {}, data:{}",tmpPath, data == null ? null : new String(data));
				return true;
			}
		}
		return false;
	}
	/**
	 * 概述：获取任务状态
	 * @param key
	 * @return
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	public static boolean getState(int key){
		if(!StateMap.containsKey(key)){
			return false;
		}
		return StateMap.get(key);
	}
}
