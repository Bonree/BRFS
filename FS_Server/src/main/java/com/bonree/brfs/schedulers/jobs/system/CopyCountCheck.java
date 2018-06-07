package com.bonree.brfs.schedulers.jobs.system;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bonree.brfs.common.service.Service;
import com.bonree.brfs.common.service.ServiceManager;
import com.bonree.brfs.common.task.TaskState;
import com.bonree.brfs.common.task.TaskType;
import com.bonree.brfs.common.utils.BrStringUtils;
import com.bonree.brfs.common.utils.Pair;
import com.bonree.brfs.common.utils.TimeUtils;
import com.bonree.brfs.configuration.ServerConfig;
import com.bonree.brfs.disknode.client.DiskNodeClient;
import com.bonree.brfs.disknode.client.HttpDiskNodeClient;
import com.bonree.brfs.disknode.server.handler.data.FileInfo;
import com.bonree.brfs.duplication.storagename.StorageNameManager;
import com.bonree.brfs.duplication.storagename.StorageNameNode;
import com.bonree.brfs.schedulers.ManagerContralFactory;
import com.bonree.brfs.schedulers.task.model.AtomTaskModel;
import com.bonree.brfs.schedulers.task.model.TaskModel;

/******************************************************************************
 * 版权信息：北京博睿宏远数据科技股份有限公司
 * Copyright: Copyright (c) 2007北京博睿宏远数据科技股份有限公司,Inc.All Rights Reserved.
 * 
 * @date 2018年5月22日 下午4:05:31
 * @Author: <a href=mailto:zhucg@bonree.com>朱成岗</a>
 * @Description:检查集群副本数量是否一致 
 *****************************************************************************
 */
public class CopyCountCheck {
	private static final Logger LOG = LoggerFactory.getLogger("CopyCheckJob");
	
	public static Map<String,List<String>> collectLossFile(List<StorageNameNode> storageNames, List<Service> services, Map<String,Long> snTimes, long granule){
		Map<StorageNameNode, List<String>> snFiles = collectionSnFiles(services, storageNames,snTimes,granule);
		if(snFiles == null|| snFiles.isEmpty()) {
			return null;
		}
		Map<StorageNameNode,Pair<List<String>, List<String>>> copyMap = calcCopyCount(snFiles);
		if(copyMap == null|| copyMap.isEmpty()){
			LOG.info("cluster data is normal !!!");
			return null;
		}
		Map<String,List<String>> results = lossFiles(copyMap);
		return results;
	}
	/**
	 * 概述：获取缺失副本的
	 * @param copyMap
	 * @return
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	public static Map<String, List<String>> lossFiles(Map<StorageNameNode,Pair<List<String>, List<String>>> copyMap){
		if(copyMap == null || copyMap.isEmpty()){
			return null;
		}
		Map<String,List<String>> lossMap = new HashMap<String,List<String>>();
		StorageNameNode sn = null;
		String snName = null;
		Pair<List<String>,List<String>> cache = null;
		List<String> losss = null;
		for(Map.Entry<StorageNameNode,Pair<List<String>, List<String>>> entry: copyMap.entrySet()){
			sn = entry.getKey();
			if(sn == null){
				continue;
			}
			snName = sn.getName();
			if(BrStringUtils.isEmpty(snName)){
				continue;
			}
			cache = entry.getValue();
			if(cache == null){
				continue;
			}
			losss = cache.getKey();
			if(losss == null || losss.isEmpty()){
				continue;
			}
			lossMap.put(snName, losss);
		}
		return lossMap;
	}
	/**
	 * 概述：
	 * @param snFiles
	 * @return
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	public static Map<StorageNameNode,Pair<List<String>, List<String>>> calcCopyCount(Map<StorageNameNode, List<String>> snFiles){
		if(snFiles == null || snFiles.isEmpty()){
			return null;
		}
		StorageNameNode sn = null;
		List files = null;
		Map<String, Integer> fileCopyCount = null;
		Pair<List<String>,List<String>> result = null;
		Map<StorageNameNode,Pair<List<String>, List<String>>> copyMap = new HashMap<StorageNameNode,Pair<List<String>, List<String>>>();
		
		for(Map.Entry<StorageNameNode, List<String>> entry : snFiles.entrySet()){
			sn = entry.getKey();
			files = entry.getValue();
			if(files == null || files.isEmpty()){
				continue;
			}
			fileCopyCount = calcFileCount(files);
			LOG.info("copycount {}",fileCopyCount);
			if(fileCopyCount == null || fileCopyCount.isEmpty()){
				continue;
			}
			result = filterLoser(fileCopyCount, sn.getReplicateCount());
			if(result == null){
				continue;
			}
			copyMap.put(sn, result);
		}
		return copyMap;
	}
		
	/**
	 * 概述：获取集群对应目录的文件
	 * @param services
	 * @param snList
	 * @param dataPath
	 * @param startTime
	 * @return
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	public static Map<StorageNameNode, List<String>> collectionSnFiles(List<Service> services, List<StorageNameNode> snList,final Map<String,Long> snTimes, long granule){
		Map<StorageNameNode,List<String>> snMap = new HashMap<>();
		DiskNodeClient client = null;
		int reCount = 0;
		String snName = null;
		String path = null;
		List<String> strs = null;
		long time = 0;
		String dirName = null;
		for(Service service : services){
			try {
				client = new HttpDiskNodeClient(service.getHost(), service.getPort());
				for(StorageNameNode sn : snList){
					reCount = sn.getReplicateCount();
					snName = sn.getName();
					if(!snTimes.containsKey(snName)) {
						continue;
					}
					time = snTimes.get(snName);
					dirName = TimeUtils.timeInterval(time, granule);
					for(int i = 1; i <=reCount; i++){
						path = File.separator+snName+File.separator+i+File.separator+dirName;
						strs = getFileList(client, path);
						if(strs == null || strs.isEmpty()) {
							continue;
						}
						if(!snMap.containsKey(sn)){
							snMap.put(sn, new ArrayList<String>());
						}
						snMap.get(sn).addAll(strs);
					}
				}
			} catch (Exception e) {
				e.printStackTrace();
			}finally{
				if(client != null){
					try {
						client.close();
					}
					catch (IOException e) {
						e.printStackTrace();
					}
				}
			}
			
		}
		return snMap;
	}
	/**
	 * 概述：获取文件名列表
	 * @param client
	 * @param path
	 * @return
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	public static List<String> getFileList(DiskNodeClient client, String path){
		if(client == null || BrStringUtils.isEmpty(path)) {
			return null;
		}
		List<FileInfo> files =client.listFiles(path, 1);
		if(files == null || files.isEmpty()) {
			return null;
		}
		List<String> fileNames = converToStringList(files, path);
		return fileNames;
	}
	 /**
	 * 概述：转换集合为str集合
	 * @param files
	 * @return
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	public static List<String> converToStringList(List<FileInfo> files,String dir){
		List<String> strs = new ArrayList<>();
		String path = null;
		String fileName = null;
		int lastIndex = 0;
		String dirName = getFileName(dir); 
		List<String> filterRd = new ArrayList<>();
		for(FileInfo file : files){
			path = file.getPath();
			fileName = getFileName(path);
			if(dirName.equals(fileName)){
				continue;
			}
			if(fileName.indexOf(".rd") > 0){
				fileName = fileName.substring(0, fileName.indexOf(".rd"));
				filterRd.add(fileName);
				continue;
			}
			strs.add(fileName);
		}
		return filterRd(strs, filterRd);
	}
	public static List<String> filterRd(final List<String> files, final List<String> rds){
		List<String> rFiles = new ArrayList<>();
		if(files == null || files.isEmpty()){
			return rFiles;
		}
		if(rds == null || rds.isEmpty()){
			rFiles.addAll(files);
			return rFiles;
		}
		for(String file : files){
			if(rds.contains(file)){
				continue;
			}
			rFiles.add(file);
		}
		return rFiles;
		
	}
	public static String getFileName(String path){
		int lastIndex = 0;
		lastIndex = path.lastIndexOf("/");
		if(lastIndex <0){
			lastIndex = path.lastIndexOf("\\");
		}
		if(lastIndex <0){
			return new String(path);
		}
		return path.substring(lastIndex+1);
	}
	/**
	 * 概述：过滤出有效的集合
	 * @param sns
	 * @param size
	 * @return
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	public static List<StorageNameNode> filterSn(List<StorageNameNode> sns, int size){
		List<StorageNameNode> filters = new ArrayList<StorageNameNode>();
		if(sns == null || sns.isEmpty()){
			return filters;
		}
		int count = 0;
		String snName = null;
		for(StorageNameNode sn : sns){
			count = sn.getReplicateCount();
			snName = sn.getName();
			if(count == 1){
				LOG.info("==== sn {} {} skip",snName,count);
				continue;
			}
			if(count >size){
				LOG.info("==== sn {} {} {} skip",snName,count, size);
				continue;
			}
			filters.add(sn);
		}
		return filters;
		
	}
	/**
	 * 概述：统计副本的个数
	 * @param files
	 * @return
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	public static Map<String, Integer> calcFileCount(final Collection<String> files){
		Map<String, Integer> filesMap = new HashMap<>();
		for(String file : files){
			if(filesMap.containsKey(file)){
				filesMap.put(file, filesMap.get(file) + 1);
			}else{
				filesMap.put(file,1);
			}
		}
		return filesMap;
	}
	/**
	 * 概述：收集副本数异常的副本
	 * @param resultMap
	 * @param filterValue
	 * @return
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	public static Pair<List<String>,List<String>>filterLoser(Map<String,Integer> resultMap, int filterValue){
		List<String> filterBiggestResult = new ArrayList<String>();
		List<String> filterLitterResult = new ArrayList<String>();
		String key = null;
		int count = 0;
		for(Map.Entry<String, Integer> entry : resultMap.entrySet()){
			count = entry.getValue();
			key = entry.getKey();
			if(filterValue == count){
				continue;
			} else	if(filterValue > count){
				filterLitterResult.add(key);
			}else if(filterValue < count){
				filterBiggestResult.add(key);
			}
		}
		Pair<List<String>,List<String>> result = new Pair<List<String>,List<String>>();
		result.setKey(filterLitterResult);
		result.setValue(filterBiggestResult);
		return result;
	}
}
