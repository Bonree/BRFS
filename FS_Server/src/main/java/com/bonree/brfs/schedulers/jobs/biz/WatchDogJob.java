package com.bonree.brfs.schedulers.jobs.biz;

import java.util.List;

import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bonree.brfs.common.utils.BrStringUtils;
import com.bonree.brfs.common.utils.TimeUtils;
import com.bonree.brfs.duplication.storageregion.StorageRegion;
import com.bonree.brfs.duplication.storageregion.StorageRegionManager;
import com.bonree.brfs.schedulers.ManagerContralFactory;
import com.bonree.brfs.schedulers.task.operation.impl.QuartzOperationStateTask;
import com.bonree.brfs.schedulers.utils.JobDataMapConstract;

public class WatchDogJob extends QuartzOperationStateTask {
	private static final Logger LOG = LoggerFactory.getLogger(WatchDogJob.class);
	@Override
	public void caughtException(JobExecutionContext context) {

	}

	@Override
	public void interrupt(){

	}

	@Override
	public void operation(JobExecutionContext context) throws Exception {
		LOG.debug("watch dog do it >>>>>>>>>>>>>>");
		JobDataMap data = context.getJobDetail().getJobDataMap();
		String zkHosts = data.getString(JobDataMapConstract.ZOOKEEPER_ADDRESS);
		String baseRoutPath = data.getString(JobDataMapConstract.BASE_ROUTE_PATH);
		String dataPath = data.getString(JobDataMapConstract.DATA_PATH);
		if(BrStringUtils.isEmpty(dataPath) || BrStringUtils.isEmpty(baseRoutPath)|| BrStringUtils.isEmpty(zkHosts)) {
			LOG.warn("config is empty !! skip watchdog");
			return;
		}
		ManagerContralFactory mcf = ManagerContralFactory.getInstance();
//		ServerIDManager sim = mcf.getSim();
		StorageRegionManager snm = mcf.getSnm();
		List<StorageRegion> sns = snm.getStorageRegionList();
		long preTime = System.currentTimeMillis();
		LOG.info("Scan {} below data !!!",TimeUtils.formatTimeStamp(preTime));
		// TODO: 4/14/20 lossssssssssssssssssssssssssssssssssssser
//		WatchDog.searchPreys(sim, sns,  dataPath, preTime);
	}

}
