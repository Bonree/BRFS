package com.bonree.brfs.schedulers.jobs.biz;

import java.io.IOException;
import java.io.RandomAccessFile;
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
import com.bonree.brfs.common.write.data.FileDecoder;
import com.bonree.brfs.disknode.utils.CheckUtils;
import com.bonree.brfs.schedulers.jobs.JobDataMapConstract;
import com.bonree.brfs.schedulers.task.model.AtomTaskModel;
import com.bonree.brfs.schedulers.task.model.AtomTaskResultModel;
import com.bonree.brfs.schedulers.task.model.BatchAtomModel;
import com.bonree.brfs.schedulers.task.model.TaskResultModel;
import com.bonree.brfs.schedulers.task.operation.impl.QuartzOperationStateWithZKTask;
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
	private static final Logger LOG = LoggerFactory.getLogger("SystemDeleteJob");
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
		LOG.info("check data //////////////////////////////////");
		JobDataMap data = context.getJobDetail().getJobDataMap();
		String currentIndex = data.getString(JobDataMapConstract.CURRENT_INDEX);
		String dataPath = data.getString(JobDataMapConstract.DATA_PATH);
		String content = data.getString(currentIndex);
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
		AtomTaskResultModel atomR = null;
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
			//调用俞朋的接口
			atomR = new AtomTaskResultModel();
			atomR.setSn(snName);
			atomR.setDir(dirName);
			if(!FileUtils.isExist(dirName)){
				LOG.warn("{} is not exists !!");
				continue;
			}
			batchResult = checkFiles(snName, dirName);
			if(batchResult == null){
				continue;
			}
			if(!batchResult.isSuccess()){
				result.setSuccess(batchResult.isSuccess());
			}
			result.addAll(batchResult.getAtoms());
		}
		//更新任务状态
		updateMapTaskMessage(context, result);
	}
	/**
	 * 概述：校验文件
	 * @param snName
	 * @param dirName
	 * @return
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	private TaskResultModel checkFiles(String snName, String dirName){
		if(!FileUtils.isExist(dirName)){
			LOG.warn("{} is not exists !!",dirName);
			return null;
		}
		TaskResultModel result = new TaskResultModel();
		AtomTaskResultModel atomR = new AtomTaskResultModel();
		boolean isSuccess = false;
		if(!FileUtils.isDirectory(dirName)){
			LOG.warn("{} is not a directory !! ", dirName);
			atomR.setSuccess(false);
			atomR.setSn(snName);
			atomR.setDir(dirName);
			result.add(atomR);
			result.setSuccess(false);
			return result;
		}
		List<String> files = FileUtils.listFilePaths(dirName);
		atomR.setSn(snName);
		atomR.setDir(dirName);
		int deleteCount = 0;
		if(files != null){
			for(String file : files){
				boolean isCheckSuccess = checkCompleted(file);
				if(isCheckSuccess){
					deleteCount ++;
				}else{
					atomR.add(file);
					atomR.setSuccess(false);
					result.setSuccess(false);
				}
			}
		}
		atomR.setOperationFileCount(deleteCount);
		result.add(atomR);
		return result;
	}
	private boolean checkCompleted(String path) {
		CRC32 crc32 = new CRC32();
		
		RandomAccessFile file = null;
		try {
			file = new RandomAccessFile(path, "r");
			//跳过2字节的头部
			file.skipBytes(2);
		
			int length = (int) (file.length() - 9);
			
			byte[] buf = new byte[8096];
			int read = -1;
			while((read = file.read(buf, 0, length)) != -1) {
				crc32.update(buf, 0, read);
				length -= read;
			}
			byte[] crcKeys = new byte[8];
			file.read(crcKeys, 0, 8);
			long crcNum = FileDecoder.validate(crcKeys);
			if(crcNum == crc32.getValue()){
				return true;
			}
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			CloseUtils.closeQuietly(file);
		}
		return false;
	}
}
