package com.bonree.brfs.schedulers.jobs.biz;

import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.bonree.brfs.email.EmailPool;
import com.bonree.mail.worker.MailWorker;
import com.bonree.mail.worker.ProgramInfo;
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
import com.bonree.brfs.schedulers.utils.WatchDog;

public class WatchSomeThingJob extends QuartzOperationStateTask {
	private static final Logger LOG = LoggerFactory.getLogger(WatchSomeThingJob.class);
	private static Map<Integer,Boolean> StateMap = new ConcurrentHashMap<Integer, Boolean>();
	public static final int RECOVERY_STATUSE = 1;
	@Override
	public void caughtException(JobExecutionContext context) {
		LOG.info("watch task error !!!");
	}

	@Override
	public void interrupt(){

	}

	@Override
	public void operation(JobExecutionContext context) throws Exception {
		JobDataMap data = context.getJobDetail().getJobDataMap();
		ManagerContralFactory mcf = ManagerContralFactory.getInstance();
		String zkHost = data.getString(JobDataMapConstract.ZOOKEEPER_ADDRESS);
		//获取client
        CuratorClient curatorClient = null;
		try {
            curatorClient = CuratorClient.getClientInstance(zkHost);
            String basePath  = mcf.getZkPath().getBaseRebalancePath();
			String tasksPath=basePath + Constants.SEPARATOR+Constants.TASKS_NODE;
			boolean isIt = isRecovery(curatorClient, tasksPath);
			// 更新map的值
			StateMap.put(RECOVERY_STATUSE, isIt);
			//发生副本迁移就删除数据
			if(isIt) {
				WatchDog.abandonFoods();
			}
		}catch (Exception e) {
			LOG.error("{}",e);
			EmailPool emailPool = EmailPool.getInstance();
			MailWorker.Builder builder = MailWorker.newBuilder(emailPool.getProgramInfo());
			builder.setModel(this.getClass().getSimpleName()+"模块服务发生问题");
			builder.setException(e);
			builder.setMessage("看门狗发生错误");
			builder.setVariable(data.getWrappedMap());
			emailPool.sendEmail(builder);
		}finally{
		    if(curatorClient != null){
		        curatorClient.close();
            }
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
		String snPath;
		List<String> cList;
		String tmpPath;
		byte[]data;
		String dataStr = null;
		for(String sn : paths){
			snPath = path + Constants.SEPARATOR +sn;
			cList = client.getChildren(snPath);
			if(cList !=null && !cList.isEmpty()){
				//TODO 防御日志
				tmpPath = snPath +"/"+ cList.get(0);
				data = client.getData(tmpPath);
				try{
					dataStr =data == null ? null : new String(data, StandardCharsets.UTF_8.name());
				} catch(UnsupportedEncodingException e){
					LOG.error("switch String error {}",e);
				}
				LOG.info("path : {}, data:{}",tmpPath, dataStr);
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
