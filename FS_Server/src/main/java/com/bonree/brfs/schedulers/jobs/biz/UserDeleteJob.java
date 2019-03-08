package com.bonree.brfs.schedulers.jobs.biz;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.bonree.brfs.common.files.impl.BRFSTimeFilter;
import com.bonree.brfs.common.utils.*;
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
 * @date 2018年5月3日 下午4:29:44
 * @Author: <a href=mailto:zhucg@bonree.com>朱成岗</a>
 * @Description:系统删除任务 
 *****************************************************************************
 */
public class UserDeleteJob extends QuartzOperationStateWithZKTask {
	public static final String DELETE_SN_ALL = "0";
	public static final String DELETE_PART = "1";
	private static final Logger LOG = LoggerFactory.getLogger(UserDeleteJob.class);
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
		String dataPath = data.getString(JobDataMapConstract.DATA_PATH);
		String content = data.getString(currentIndex);
		// 获取当前执行的任务类型
		if(BrStringUtils.isEmpty(content)) {
			LOG.debug("batch data is empty !!!");
			return ;
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
		String snName;
		TaskResultModel result = new TaskResultModel();
		AtomTaskResultModel usrResult;
		List<String> dSns = new ArrayList<String>();
		String operation;
		for(AtomTaskModel atom : atoms){
			snName = atom.getStorageName();
			if("1".equals(currentIndex)) {
				operation = atom.getTaskOperation();
				LOG.debug("task operation {} ", DELETE_SN_ALL.equals(operation) ? "Delete_Storage_Region" : "Delete_Part_Of_Storage_Region_Data");
				if(DELETE_SN_ALL.equals(operation)) {
					dSns.add(snName);
				}
			}
			usrResult = deleteFiles(atom, dataPath);
			if (usrResult == null) {
				continue;
			}
			if (!usrResult.isSuccess()) {
				result.setSuccess(false);
			}
			result.add(usrResult);
			
		}
		if("1".equals(currentIndex)) {
			for(String sn : dSns) {
				if(FileUtils.deleteDir(dataPath+"/"+sn, true)) {
					LOG.debug("deltete {} successfull", sn);
				}else {
					result.setSuccess(false);
				}
			}
		}
		//更新任务状态
		TaskStateLifeContral.updateMapTaskMessage(context, result);
	}
	/**
	 * 概述：封装执行结果
	 * @param atom
	 * @param dataPath
	 * @return
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	public AtomTaskResultModel deleteFiles(AtomTaskModel atom, String dataPath){
		if(atom == null) {
			return null;
		}
		String snName = atom.getStorageName();
		long startTime = TimeUtils.getMiles(atom.getDataStartTime(), TimeUtils.TIME_MILES_FORMATE);
		long endTime = TimeUtils.getMiles(atom.getDataStopTime(), TimeUtils.TIME_MILES_FORMATE);
		AtomTaskResultModel atomR = new AtomTaskResultModel();
		atomR.setDataStartTime(TimeUtils.formatTimeStamp(startTime, TimeUtils.TIME_MILES_FORMATE));
		atomR.setDataStopTime(TimeUtils.formatTimeStamp(endTime, TimeUtils.TIME_MILES_FORMATE));
		atomR.setPartNum(atom.getPatitionNum());
		atomR.setSn(snName);
		Map<String,String> snMap = new HashMap<>();
		snMap.put(BRFSPath.STORAGEREGION, snName);
		List<BRFSPath> deleteDirs = BRFSFileUtil.scanBRFSFiles(dataPath,snMap,snMap.size(),new BRFSTimeFilter(startTime,endTime));
		LOG.debug("collection {}_{} dirs {}",atom.getDataStartTime(),atom.getDataStopTime(),deleteDirs);
		if(deleteDirs == null || deleteDirs.isEmpty()) {
			atomR.setOperationFileCount(0);
			return atomR;
		}
		boolean isSuccess = true;
		for(BRFSPath deletePath : deleteDirs) {
			isSuccess = isSuccess && FileUtils.deleteDir(dataPath+FileUtils.FILE_SEPARATOR+deletePath.toString(), true);
			LOG.debug("delete [{}], status [{}]",deletePath, isSuccess);
		}
		atomR.setOperationFileCount(deleteDirs.size());
		atomR.setSuccess(isSuccess);
		return atomR;
	}
}
