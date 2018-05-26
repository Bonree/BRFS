package com.bonree.brfs.schedulers.jobs.biz;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;

import com.bonree.brfs.common.task.TaskState;
import com.bonree.brfs.common.task.TaskType;
import com.bonree.brfs.common.utils.JsonUtils;
import com.bonree.brfs.schedulers.jobs.JobDataMapConstract;
import com.bonree.brfs.schedulers.task.manager.MetaTaskManagerInterface;
import com.bonree.brfs.schedulers.task.model.AtomTaskModel;
import com.bonree.brfs.schedulers.task.model.BatchAtomModel;
import com.bonree.brfs.schedulers.task.model.TaskModel;
import com.bonree.brfs.schedulers.task.model.TaskResultModel;
import com.bonree.brfs.schedulers.task.operation.impl.TaskStateLifeContral;

public class BatchTaskFactory {
	/**
	 * 概述：
	 * @param release
	 * @param task
	 * @param taskName
	 * @param serverId
	 * @param batchSize
	 * @return
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	public static Map<String,String> createBatch(MetaTaskManagerInterface release, TaskModel task, String taskName, String serverId,int batchSize){
		Map<String,String> batchDatas = new HashMap<String,String>();
		if(task == null){
			return batchDatas;
		}
		TaskType taskType = TaskType.valueOf(task.getTaskType());
		List<AtomTaskModel> atoms = convernTaskModel(task);
		if (atoms == null|| atoms.isEmpty()) {
			return batchDatas;
		}
		int size = atoms.size(); 
		int count = size / batchSize;
		BatchAtomModel batch = null;
		List<AtomTaskModel> tmp = null;
		batchDatas.put(JobDataMapConstract.CURRENT_INDEX, count + "");
		int index = 0;
		for (int i = 1; i <= count; i += count) {
			batch = new BatchAtomModel();
			if (index + count <= size) {
				tmp = atoms.subList(index, index + count);
			}
			else if (size > 0) {
				tmp = atoms.subList(index, size - 1);
			}
			else {
				tmp = new ArrayList<AtomTaskModel>();
			}
			batch.addAll(tmp);
			batchDatas.put(i + "", JsonUtils.toJsonString(batch));
			index = index + count;
		}
		return batchDatas;
	}
	
	public static List<AtomTaskModel> convernTaskModel(TaskModel task){
		List<AtomTaskModel> atoms = new ArrayList<AtomTaskModel>();
		boolean isException = TaskState.EXCEPTION.code() == task.getTaskState();
//		if(isException){
//			TaskResultModel result = task.getResult();
//			if(result == null){
//				atoms.addAll(task.getAtomList());
//				return atoms;
//			}
//			List<AtomTaskResultModel> atomRs = result.getAtoms();
//			if(atomRs == null || atomRs.isEmpty()){
//				atoms.addAll(task.getAtomList());
//				return atoms;
//			}
//			AtomTaskModel atomT = null;
//			for(AtomTaskResultModel atomR : atomRs){
//				if(atomR.getFiles() == null || atomR.getFiles().isEmpty()){
//					continue;
//				}
//				atomT = new AtomTaskModel();
//				atomT.setFiles(atomR.getFiles());
//				atomT.setStorageName(atomR.getSn());
//				atoms.add(atomT);
//			}
//			return atoms;
//		}
		List<AtomTaskModel> tasks = task.getAtomList();
		if(tasks == null || tasks.isEmpty()){
			return atoms;
		}
		atoms.addAll(tasks);
		return atoms;
	}
}
