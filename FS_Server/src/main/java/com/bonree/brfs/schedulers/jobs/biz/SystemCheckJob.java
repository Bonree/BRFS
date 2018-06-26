package com.bonree.brfs.schedulers.jobs.biz;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.List;
import java.util.zip.CRC32;

import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import org.quartz.UnableToInterruptJobException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bonree.brfs.common.task.TaskState;
import com.bonree.brfs.common.utils.BrStringUtils;
import com.bonree.brfs.common.utils.CloseUtils;
import com.bonree.brfs.common.utils.FileUtils;
import com.bonree.brfs.common.utils.JsonUtils;
import com.bonree.brfs.common.write.data.FSCode;
import com.bonree.brfs.common.write.data.FileDecoder;
import com.bonree.brfs.disknode.utils.CheckUtils;
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
		LOG.info("----------> check task work");
		JobDataMap data = context.getJobDetail().getJobDataMap();
		String currentIndex = data.getString(JobDataMapConstract.CURRENT_INDEX);
		String dataPath = data.getString(JobDataMapConstract.DATA_PATH);
		String content = data.getString(currentIndex);
		LOG.info("batch data :{}", content);
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
		String dirName = null;
		TaskResultModel result = new TaskResultModel();
		TaskResultModel batchResult = null;
		boolean isException = false;
		for(AtomTaskModel atom : atoms){
			snName = atom.getStorageName();
			dirName = atom.getDirName();
			if(BrStringUtils.isEmpty(snName)){
				LOG.warn("sn is empty !!!");
				continue;
			}
			if(BrStringUtils.isEmpty(dirName)){
				LOG.warn("dir is empty !!!");
				continue;
			}
			batchResult = checkFiles(snName, dirName, dataPath);
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
	 * 概述：校验文件
	 * @param snName
	 * @param dirName
	 * @return
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	private TaskResultModel checkFiles(String snName, String dirName, String dataPath){
		String path = dataPath + File.separator + dirName;
		TaskResultModel result = new TaskResultModel();
		AtomTaskResultModel atomR = new AtomTaskResultModel();
		atomR.setDir(dirName);
		atomR.setSn(snName);
		List<String> errors = FileCollection.checkDirs(path);
		if(errors == null || errors.isEmpty()) {
			atomR.setSuccess(true);
			result.setSuccess(true);
		}else {
			atomR.setSuccess(false);
			result.setSuccess(false);
			atomR.addAll(errors);
		}
		result.add(atomR);
		return result;
	}
	
}
