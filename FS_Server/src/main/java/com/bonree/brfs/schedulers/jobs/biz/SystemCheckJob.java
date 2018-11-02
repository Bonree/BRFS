package com.bonree.brfs.schedulers.jobs.biz;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.bonree.brfs.common.utils.*;
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
	private static final Logger LOG = LoggerFactory.getLogger("SystemCheckJob");
	@Override
	public void caughtException(JobExecutionContext context) {
		LOG.info("Error ......   ");
	}

	@Override
	public void interrupt() throws UnableToInterruptJobException {
		LOG.info("interrupt ......   " );
		
	}

	@Override
	public void operation(JobExecutionContext context) throws Exception {
		LOG.info("check task work");
		JobDataMap data = context.getJobDetail().getJobDataMap();
		String currentIndex = data.getString(JobDataMapConstract.CURRENT_INDEX);
		String dataPath = data.getString(JobDataMapConstract.DATA_PATH);
		String content = data.getString(currentIndex);
		
		if(BrStringUtils.isEmpty(content)){
			LOG.warn("batch data is empty !!!");
			return;
		}
		BatchAtomModel batch = JsonUtils.toObject(content, BatchAtomModel.class);
		if(batch == null){
			LOG.warn("batch data is empty !!!");
			return;
		}
		List<AtomTaskModel> atoms = batch.getAtoms();
		if(atoms == null || atoms.isEmpty()){
			LOG.warn("atom task is empty !!!");
			return;
		}
		String snName = null;
		TaskResultModel result = new TaskResultModel();
		TaskResultModel batchResult = null;
		for(AtomTaskModel atom : atoms){
			snName = atom.getStorageName();
			if(BrStringUtils.isEmpty(snName)){
				LOG.warn("sn is empty !!!");
				continue;
			}
			batchResult =checkFiles(atom, dataPath);
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
	public TaskResultModel checkFiles(AtomTaskModel atom ,String dataPath){
		String snName  = atom.getStorageName();
		int partitionNum = atom.getPatitionNum();
		long startTime = TimeUtils.getMiles(atom.getDataStartTime(),TimeUtils.TIME_MILES_FORMATE);
		long endTime = TimeUtils.getMiles(atom.getDataStopTime(),TimeUtils.TIME_MILES_FORMATE);
//		List<String> partDirs = LocalFileUtils.getPartitionDirs(dataPath, snName, partitionNum);
//		List<String> checkDirs = LocalFileUtils.collectTimeDirs(partDirs, startTime, endTime, 1, false);
//		LOG.debug("CHECKJOB-0 start: {}, end : {}", atom.getDataStartTime(), atom.getDataStopTime());
//		LOG.debug("CHECKJOB-1 list dir :{}", partDirs);
//		LOG.debug("CHECKJOB-2 check List: {}", checkDirs);
        Map<String,String> snMap = new HashMap<>();
        snMap.put(BRFSPath.STORAGEREGION,snName);
        List<BRFSPath> eFiles = BRFSFileUtil.scanBRFSFiles(dataPath,snMap,snMap.size(),new BRFSCheckFilter(startTime,endTime));
        List<String> errors = new ArrayList<>();
        if(eFiles != null ){
            for(BRFSPath brfsPath: eFiles){
                errors.add(brfsPath.getFileName());
            }
        }
        TaskResultModel result = new TaskResultModel();
		AtomTaskResultModel  atomR = AtomTaskResultModel.getInstance(errors, snName, startTime, endTime, "", partitionNum);
        result.add(atomR);
		//  for(String checkDir :checkDirs) {
//            errors = FileCollection.checkDirs(checkDir);
//            LOG.info("CHECKJOB-3 error List: {}", errors);
//			if(errors !=null && !errors.isEmpty()) {
//				atomR.setSuccess(false);
//				result.setSuccess(false);
//			}
//            atomR = AtomTaskResultModel.getInstance(errors, snName, startTime, endTime, "", partitionNum);
//			result.add(atomR);
//		}
		LOG.debug("result : {}",JsonUtils.toJsonStringQuietly(result));
		return result;
	}
	
}
