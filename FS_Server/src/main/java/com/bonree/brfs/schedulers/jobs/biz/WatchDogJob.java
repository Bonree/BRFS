package com.bonree.brfs.schedulers.jobs.biz;

import java.util.List;

import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import org.quartz.UnableToInterruptJobException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bonree.brfs.common.service.Service;
import com.bonree.brfs.common.service.ServiceManager;
import com.bonree.brfs.duplication.storagename.StorageNameManager;
import com.bonree.brfs.duplication.storagename.StorageNameNode;
import com.bonree.brfs.rebalance.route.SecondIDParser;
import com.bonree.brfs.schedulers.ManagerContralFactory;
import com.bonree.brfs.schedulers.jobs.JobDataMapConstract;
import com.bonree.brfs.schedulers.task.operation.impl.QuartzOperationStateTask;
import com.bonree.brfs.server.identification.ServerIDManager;

public class WatchDogJob extends QuartzOperationStateTask {
	private static final Logger LOG = LoggerFactory.getLogger("WatchDogJob");
	@Override
	public void caughtException(JobExecutionContext context) {

	}

	@Override
	public void interrupt() throws UnableToInterruptJobException {

	}

	@Override
	public void operation(JobExecutionContext context) throws Exception {
		JobDataMap data = context.getJobDetail().getJobDataMap();
		String currentIndex = data.getString(JobDataMapConstract.CURRENT_INDEX);
		String zkHosts = data.getString(JobDataMapConstract.ZOOKEEPER_ADDRESS);
		String baseRoutPath = data.getString(JobDataMapConstract.BASE_ROUTE_PATH);
		String dataPath = data.getString(JobDataMapConstract.DATA_PATH);
		ManagerContralFactory mcf = ManagerContralFactory.getInstance();
		ServerIDManager sim = mcf.getSim();
		ServiceManager sm = mcf.getSm();
		Service localServer = sm.getServiceById(mcf.getGroupName(), mcf.getServerId());
		StorageNameManager snm = mcf.getSnm();
		List<StorageNameNode> sns = snm.getStorageNameNodeList();
		WatchDog.searchPreys(sim, sns, zkHosts, dataPath, dataPath, -1, 3600000);
	}

}
