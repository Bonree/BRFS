package com.bonree.brfs.schedulers.jobs.biz;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import org.quartz.UnableToInterruptJobException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bonree.brfs.common.task.TaskState;
import com.bonree.brfs.common.task.TaskType;
import com.bonree.brfs.common.utils.BrStringUtils;
import com.bonree.brfs.common.utils.FileUtils;
import com.bonree.brfs.common.utils.JsonUtils;
import com.bonree.brfs.common.utils.Pair;
import com.bonree.brfs.schedulers.jobs.JobDataMapConstract;
import com.bonree.brfs.schedulers.task.model.AtomTaskModel;
import com.bonree.brfs.schedulers.task.model.AtomTaskResultModel;
import com.bonree.brfs.schedulers.task.model.BatchAtomModel;
import com.bonree.brfs.schedulers.task.model.TaskResultModel;
import com.bonree.brfs.schedulers.task.operation.impl.QuartzOperationStateWithZKTask;
import com.bonree.brfs.schedulers.task.operation.impl.TaskStateLifeContral;
/******************************************************************************
 * 版权信息：北京博睿宏远数据科技股份有限公司
 * Copyright: Copyright (c) 2007北京博睿宏远数据科技股份有限公司,Inc.All Rights Reserved.
 * @param <AtomTaskModel>
 * 
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
		LOG.info("Error ......   ");
		
	}

	@Override
	public void interrupt() throws UnableToInterruptJobException {
		LOG.info("interrupt ......   " );
		
	}

	@Override
	public void operation(JobExecutionContext context) throws Exception {
		JobDataMap data = context.getJobDetail().getJobDataMap();
		String currentIndex = data.getString(JobDataMapConstract.CURRENT_INDEX);
		String dataPath = data.getString(JobDataMapConstract.DATA_PATH);
		String content = data.getString(currentIndex);
		LOG.info("user delete batch {}",content);
		// 获取当前执行的任务类型
		int taskType = data.getInt(JobDataMapConstract.TASK_TYPE);
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
		String dirName = null;
		TaskResultModel result = new TaskResultModel();
		TaskResultModel batchResult = null;
		AtomTaskResultModel usrResult = null;
		List<String> dSns = new ArrayList<String>();
		String operation = null;
		for(AtomTaskModel atom : atoms){
			snName = atom.getStorageName();
			dirName = atom.getDirName();
			if("1".equals(currentIndex)) {
				operation = atom.getTaskOperation();
				LOG.info("task operation {} source:{}",operation ,DELETE_SN_ALL);
				if(DELETE_SN_ALL.equals(operation)) {
					dSns.add(snName);
				}
			}
			
			if(BrStringUtils.isEmpty(snName)){
				LOG.warn("sn is empty !!!");
				continue;
			}
			if(BrStringUtils.isEmpty(dirName)){
				LOG.warn("dir is empty !!!");
				continue;
			}
			usrResult = deleteFiles(snName, dirName, dataPath, atom.getGranule());
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
					LOG.info("deltete {} successfull", sn);
				}else {
					result.setSuccess(false);
				}
			}
		}
		//更新任务状态
		TaskStateLifeContral.updateMapTaskMessage(context, result);
	}
	/**
	 * 概述：删除指定目录的任务信息
	 * @param snName
	 * @param dirName
	 * @return
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	private AtomTaskResultModel deleteFiles(String snName, String dirName,String dataPath,long granule){
		String path = dataPath + File.separator + dirName;
		if(!FileUtils.isExist(path)){
			LOG.warn("{} is not exists !!",path);
			return null;
		}
		AtomTaskResultModel atomR = new AtomTaskResultModel();
		boolean isSuccess = false;
		isSuccess = FileUtils.deleteDir(path,true);
		atomR.setSn(snName);
		atomR.setGranule(granule);
		atomR.setDir(dirName);
		atomR.setSuccess(isSuccess);
		atomR.setOperationFileCount(1);
		return atomR;
	}
	public static String coveryPath(String path){
		String paths = new String(path);
		int index = paths.lastIndexOf("/");
		if(index == path.length() -1){
			paths = paths.substring(0, index);
		}
		index = paths.lastIndexOf("\\");
		if(index == path.length() -1){
			paths = paths.substring(0, index);
		}
		return paths;
	
	}
}
