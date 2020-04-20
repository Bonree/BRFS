
package com.bonree.brfs.schedulers.jobs.biz;

import java.util.*;

import ch.qos.logback.core.util.FileUtil;
import com.bonree.brfs.common.files.impl.BRFSTimeFilter;
import com.bonree.brfs.common.utils.*;
import com.bonree.brfs.identification.LocalPartitionInterface;
import com.bonree.brfs.identification.impl.DiskDaemon;
import com.bonree.brfs.partition.model.LocalPartitionInfo;
import com.bonree.brfs.schedulers.ManagerContralFactory;
import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import org.quartz.UnableToInterruptJobException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bonree.brfs.schedulers.utils.JobDataMapConstract;
import com.bonree.brfs.schedulers.task.model.AtomTaskModel;
import com.bonree.brfs.schedulers.task.model.AtomTaskResultModel;
import com.bonree.brfs.schedulers.task.model.BatchAtomModel;
import com.bonree.brfs.schedulers.task.model.TaskResultModel;
import com.bonree.brfs.schedulers.task.operation.impl.QuartzOperationStateWithZKTask;
import com.bonree.brfs.schedulers.utils.TaskStateLifeContral;

/******************************************************************************
 * 版权信息：北京博睿宏远数据科技股份有限公司
 * Copyright: Copyright (c) 2007北京博睿宏远数据科技股份有限公司,Inc.All Rights Reserved.
 *
 * @date 2018年5月3日 下午4:29:44
 * @Author: <a href=mailto:zhucg@bonree.com>朱成岗</a>
 * @Description:系统删除任务 
 *****************************************************************************
 */
public class SystemDeleteJob extends QuartzOperationStateWithZKTask {
	private static final Logger LOG = LoggerFactory.getLogger(SystemDeleteJob.class);



	@Override
	public void interrupt(){
	}

	@Override
	public void operation(JobExecutionContext context) throws Exception {
		LOG.debug("----------> system delete work");
		JobDataMap data = context.getJobDetail().getJobDataMap();
		String currentIndex = data.getString(JobDataMapConstract.CURRENT_INDEX);
		String content = data.getString(currentIndex);
		LOG.debug("batch {}", content);
		// 获取当前执行的任务类型
		BatchAtomModel batch = JsonUtils.toObject(content, BatchAtomModel.class);
		if (batch == null) {
			LOG.debug("batch data is empty !!!");
			return;
		}

		List<AtomTaskModel> atoms = batch.getAtoms();
		if (atoms == null || atoms.isEmpty()) {
			LOG.debug("atom task is empty !!!");
			return;
		}
		String snName;
		TaskResultModel result = new TaskResultModel();
		
		AtomTaskResultModel usrResult;
		DiskDaemon daemon = ManagerContralFactory.getInstance().getDaemon();
		for (AtomTaskModel atom : atoms) {
			snName = atom.getStorageName();
			if (BrStringUtils.isEmpty(snName)) {
				LOG.debug("sn is empty !!!");
				continue;
			}
			usrResult = deleteDirs(atom,daemon);
			if (usrResult == null) {
				continue;
			}
			if (!usrResult.isSuccess()) {
				result.setSuccess(false);
			}
			result.add(usrResult);
		}
		//更新任务状态
		TaskStateLifeContral.updateMapTaskMessage(context, result);
	}
	/**
	 * 概述：封装结果
	 * @param atom
	 * @return
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	public AtomTaskResultModel deleteDirs(AtomTaskModel atom, DiskDaemon diskDaemon) {
		String snName = atom.getStorageName();
		int patitionNum = atom.getPatitionNum();
		long granule = atom.getGranule();
		long startTime = TimeUtils.getMiles(atom.getDataStartTime(), TimeUtils.TIME_MILES_FORMATE);
		long endTime = TimeUtils.getMiles(atom.getDataStopTime(), TimeUtils.TIME_MILES_FORMATE);
		AtomTaskResultModel atomR = AtomTaskResultModel.getInstance(null, snName, startTime, endTime, "", patitionNum);
        Map<String,String> snMap = new HashMap<>();
        snMap.put(BRFSPath.STORAGEREGION, snName);
        Collection<LocalPartitionInfo> paths = diskDaemon.getPartitions();
        for(LocalPartitionInfo path : paths){

			List<BRFSPath> deleteDirs = BRFSFileUtil.scanBRFSFiles(path.getDataDir(),snMap,snMap.size(),new BRFSTimeFilter(0,endTime));
			if(deleteDirs == null || deleteDirs.isEmpty()) {
				LOG.debug("delete dir {} - {} is empty ",TimeUtils.timeInterval(startTime,granule), TimeUtils.timeInterval(endTime,granule));
				continue;
			}
			atomR.setOperationFileCount(atomR.getOperationFileCount()+deleteDirs.size());
			boolean isSuccess = true;
			for(BRFSPath deleteDir : deleteDirs) {

				isSuccess = isSuccess && FileUtils.deleteDir(path.getDataDir()+ FileUtils.FILE_SEPARATOR+deleteDir.toString(), true);
				LOG.debug("delete :{} status :{} ",deleteDir, isSuccess);
			}
			atomR.setSuccess(atomR.isSuccess()&&isSuccess);
		}
		return atomR;
	}
}
