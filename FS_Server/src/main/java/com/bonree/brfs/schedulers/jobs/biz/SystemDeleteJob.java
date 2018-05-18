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
public class SystemDeleteJob extends QuartzOperationStateWithZKTask {
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
		LOG.info("----------> system delete work");
		JobDataMap data = context.getJobDetail().getJobDataMap();
		String currentIndex = data.getString(JobDataMapConstract.CURRENT_INDEX);
		String dataPath = data.getString(JobDataMapConstract.DATA_PATH);
		String content = data.getString(currentIndex);
		// 获取当前执行的任务类型
		int taskType = data.getInt(JobDataMapConstract.TASK_TYPE);
		boolean isuser = TaskType.USER_DELETE.code() == taskType;
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
			if(!FileUtils.isExist(dirName)){
				LOG.warn("{} is not exists !!");
				continue;
			}
			if(isuser){
				usrResult = deleteFiles(snName, dirName);
				if(usrResult == null){
					continue;
				}
				if(!usrResult.isSuccess()){
					result.setSuccess(false);
				}
				result.add(usrResult);
			}else{
				batchResult = deleteDirs(snName, dirName);
				if(batchResult == null){
					continue;
				}
				if(!batchResult.isSuccess()){
					result.setSuccess(false);
				}
				result.addAll(batchResult.getAtoms());
			}
		}
		//更新任务状态
		updateMapTaskMessage(context, result);
	}
	/**
	 * 概述：删除之前的数据
	 * @param snName
	 * @param dirName
	 * @return
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	private TaskResultModel deleteDirs(String snName, String dirName){
		TaskResultModel result = new TaskResultModel();
		AtomTaskResultModel  atom = null;
		if(!FileUtils.isDirectory(dirName)){
			atom = deleteFiles(snName, dirName);
			if(atom != null){
				result.add(atom);
				result.setSuccess(atom.isSuccess());
			}
			return result;
		}
		File parent = new File(dirName).getParentFile();
		String path = parent.getAbsolutePath();
		List<String> dirs = FileUtils.listFilePaths(path);
		List<String> filters = filterFiles(dirName, dirs);
		for(String file : filters){
			atom = deleteFiles(snName, file);
			if(atom == null){
				continue;
			}
			if(!atom.isSuccess()){
				result.setSuccess(false);
				result.add(atom);
			}
		}
		return result;
	}
	/**
	 * 概述：过滤出需要删除的文件
	 * @param dirName
	 * @param dirs
	 * @return
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	public List<String> filterFiles(String dirName, List<String> dirs){
		List<String> files = new ArrayList<String>();
		//升序排列任务
		Collections.sort(dirs, new Comparator<String>() {
			public int compare(String o1, String o2) {
				return o1.compareTo(o2);
			}
		});
		//获取文件路径，统一分割符。
		String path = new File(dirName).getAbsolutePath();
		int index = dirs.indexOf(path);
		if(index < 0){
			files.add(dirName);
			return files;
		}
		String tmpDir = null;
		for(int i = 0; i <= index; i++){
			tmpDir = dirs.get(i);
			files.add(tmpDir);
		}
		return files;
	}
	/**
	 * 概述：删除指定目录的任务信息
	 * @param snName
	 * @param dirName
	 * @return
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	private AtomTaskResultModel deleteFiles(String snName, String dirName){
		if(!FileUtils.isExist(dirName)){
			LOG.warn("{} is not exists !!",dirName);
			return null;
		}
		AtomTaskResultModel atomR = new AtomTaskResultModel();
		boolean isSuccess = false;
		if(!FileUtils.isDirectory(dirName)){
			LOG.warn("{} is not a directory !! will delete", dirName);
			isSuccess = FileUtils.deleteFile(dirName);
			atomR.setSuccess(isSuccess);
			atomR.setSn(snName);
			atomR.setDir(dirName);
			return atomR;
		}
		List<String> files = FileUtils.listFilePaths(dirName);
		atomR.setSn(snName);
		atomR.setDir(dirName);
		int deleteCount = 0;
		if(files != null){
			for(String file : files){
				boolean isDelete = FileUtils.deleteFile(file);
				if(isDelete){
					deleteCount ++;
				}else{
					atomR.add(file);
					atomR.setSuccess(false);
				}
			}
		}
		atomR.setOperationFileCount(deleteCount);
		return atomR;
	}
	public static void main(String[] args) {
		String dir = "D:/tmp/tmp/test/styles";
		File file = new File(dir);
		String path = file.getParentFile().getAbsolutePath();
		System.out.println(path);
		SystemDeleteJob a = new SystemDeleteJob();
		List<String> dirs = FileUtils.listFilePaths(path);
		System.out.println(dirs);
		List<String> tmp = a.filterFiles(dir, dirs);
		System.out.println(dirs);
		System.out.println(tmp);
		TaskResultModel t = a.deleteDirs("sn", dir);
		System.out.println(JsonUtils.toJsonString(t));
	}
}
