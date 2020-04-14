package com.bonree.brfs.schedulers.jobs.biz;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.bonree.brfs.common.utils.*;
import com.bonree.brfs.identification.impl.DiskDaemon;
import com.bonree.brfs.partition.model.LocalPartitionInfo;
import com.bonree.brfs.schedulers.ManagerContralFactory;
import com.bonree.brfs.schedulers.utils.*;
import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import org.quartz.UnableToInterruptJobException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bonree.brfs.schedulers.task.model.AtomTaskModel;
import com.bonree.brfs.schedulers.task.model.AtomTaskResultModel;
import com.bonree.brfs.schedulers.task.model.BatchAtomModel;
import com.bonree.brfs.schedulers.task.model.TaskResultModel;
import com.bonree.brfs.schedulers.task.operation.impl.QuartzOperationStateWithZKTask;

/******************************************************************************
 * 版权信息：北京博睿宏远数据科技股份有限公司
 * Copyright: Copyright (c) 2007北京博睿宏远数据科技股份有限公司,Inc.All Rights Reserved.
 * @date 2018年5月3日 下午4:29:44
 * @Author: <a href=mailto:zhucg@bonree.com>朱成岗</a>
 * @Description:系统删除任务 
 *****************************************************************************
 */
public class SystemCheckJob extends QuartzOperationStateWithZKTask {
	private static final Logger LOG = LoggerFactory.getLogger(SystemCheckJob.class);
	@Override
	public void caughtException(JobExecutionContext context) {
		LOG.info("Error ......   ");
	}

	@Override
	public void interrupt(){


	}

	@Override
	public void operation(JobExecutionContext context) throws Exception {
		LOG.debug("check task work");
		JobDataMap data = context.getJobDetail().getJobDataMap();
		String currentIndex = data.getString(JobDataMapConstract.CURRENT_INDEX);
		String content = data.getString(currentIndex);
		
		if(BrStringUtils.isEmpty(content)){
			LOG.debug("batch data is empty !!!");
			return;
		}
		BatchAtomModel batch = JsonUtils.toObject(content, BatchAtomModel.class);
		if(batch == null){
			LOG.debug("batch data is empty !!!");
			return;
		}
		List<AtomTaskModel> atoms = batch.getAtoms();
		if(atoms == null || atoms.isEmpty()){
			LOG.debug("atom task is empty !!!");
			return;
		}
		String snName = null;
		TaskResultModel result = new TaskResultModel();
		TaskResultModel batchResult = null;
		DiskDaemon daemon = ManagerContralFactory.getInstance().getDaemon();
		for(AtomTaskModel atom : atoms){
			snName = atom.getStorageName();
			if(BrStringUtils.isEmpty(snName)){
				LOG.warn("sn is empty !!!");
				continue;
			}
			batchResult =checkFiles(atom, daemon);
			if(batchResult == null){
				continue;
			}
			if(!batchResult.isSuccess()){
				result.setSuccess(batchResult.isSuccess());
			}
			result.addAll(batchResult.getAtoms());
		}
		//更新任务状态
		TaskStateLifeContral.updateMapTaskMessage(context, result);
	}
    /**
     * @param atom
     * @param dataPath
     * @return
     */
	public TaskResultModel checkFiles(AtomTaskModel atom ,DiskDaemon daemon){
		String snName  = atom.getStorageName();
		int partitionNum = atom.getPatitionNum();
		long startTime = TimeUtils.getMiles(atom.getDataStartTime(),TimeUtils.TIME_MILES_FORMATE);
		long endTime = TimeUtils.getMiles(atom.getDataStopTime(),TimeUtils.TIME_MILES_FORMATE);
		List<String> errors = new ArrayList<>();
		for(LocalPartitionInfo path : daemon.getPartitions()){
			Map<String,String> snMap = new HashMap<>();
			snMap.put(BRFSPath.STORAGEREGION,snName);
			List<BRFSPath> eFiles = BRFSFileUtil.scanBRFSFiles(path.getDataDir(),snMap,snMap.size(),new BRFSCheckFilter(startTime,endTime));
			if(eFiles != null ){
				for(BRFSPath brfsPath: eFiles){
					errors.add(brfsPath.getFileName());
				}
			}
		}
        TaskResultModel result = new TaskResultModel();
		AtomTaskResultModel  atomR = AtomTaskResultModel.getInstance(errors, snName, startTime, endTime, "", partitionNum);
        if(errors !=null && !errors.isEmpty()) {
            atomR.setSuccess(false);
            result.setSuccess(false);
        }
		result.add(atomR);
		LOG.debug("result : {}",JsonUtils.toJsonStringQuietly(result));
		return result;
	}
	
}
