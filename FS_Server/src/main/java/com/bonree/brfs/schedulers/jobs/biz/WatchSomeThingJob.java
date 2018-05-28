package com.bonree.brfs.schedulers.jobs.biz;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import org.quartz.UnableToInterruptJobException;

import com.bonree.brfs.common.ZookeeperPaths;
import com.bonree.brfs.common.rebalance.Constants;
import com.bonree.brfs.common.zookeeper.curator.CuratorClient;
import com.bonree.brfs.schedulers.ManagerContralFactory;
import com.bonree.brfs.schedulers.jobs.JobDataMapConstract;
import com.bonree.brfs.schedulers.task.operation.impl.QuartzOperationStateTask;

public class WatchSomeThingJob extends QuartzOperationStateTask {
	private static Map<Integer,Boolean> StateMap = new ConcurrentHashMap<Integer, Boolean>();
	public static int RECOVERY_STATUSE = 1;
	private static CuratorClient curatorClient =null;
	@Override
	public void caughtException(JobExecutionContext context) {

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
			//获取监听的目录
			ZookeeperPaths zkPaths = ZookeeperPaths.create(groupName, zkHost);
			String rebalances = zkPaths.getBaseRebalancePath();
			String tasksPath=rebalances + Constants.SEPARATOR+Constants.TASKS_NODE;
			boolean isIt = isRecovery(curatorClient, tasksPath);
			// 更新map的值
			this.StateMap.put(RECOVERY_STATUSE, isIt);
		}catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
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
		for(String sn : paths){
			snPath = path + Constants.SEPARATOR +sn;
			cList = client.getChildren(snPath);
			if(cList !=null && !cList.isEmpty()){
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
