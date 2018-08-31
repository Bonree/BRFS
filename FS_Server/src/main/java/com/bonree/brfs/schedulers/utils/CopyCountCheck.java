package com.bonree.brfs.schedulers.utils;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bonree.brfs.common.service.Service;
import com.bonree.brfs.common.utils.BrStringUtils;
import com.bonree.brfs.common.utils.Pair;
import com.bonree.brfs.common.utils.TimeUtils;
import com.bonree.brfs.disknode.client.DiskNodeClient;
import com.bonree.brfs.disknode.server.handler.data.FileInfo;
import com.bonree.brfs.duplication.storageregion.StorageRegion;

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
	private static final Logger LOG = LoggerFactory.getLogger("CopyCountCheck");
	/***
	 * 概述：获取文件缺失的sn
	 * @param storageNames
	 * @param services
	 * @param snTimes
	 * @return
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	public static Map<String,List<String>> collectLossFile(List<StorageRegion> storageNames, List<Service> services, Map<String,Long> snTimes){
		Map<StorageRegion, List<String>> snFiles = collectionSnFiles(services, storageNames,snTimes);
		if(snFiles == null|| snFiles.isEmpty()) {
			LOG.debug("<collectLossFile> collection files is empty");
			return null;
		}
		Map<StorageRegion,Pair<List<String>, List<String>>> copyMap = calcCopyCount(snFiles);
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
	public static Map<String, List<String>> lossFiles(Map<StorageRegion,Pair<List<String>, List<String>>> copyMap){
		if(copyMap == null || copyMap.isEmpty()){
			return null;
		}
		Map<String,List<String>> lossMap = new HashMap<String,List<String>>();
		StorageRegion sn = null;
		String snName = null;
		Pair<List<String>,List<String>> cache = null;
		List<String> losss = null;
		for(Map.Entry<StorageRegion,Pair<List<String>, List<String>>> entry: copyMap.entrySet()){
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
			losss = cache.getFirst();
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
	public static Map<StorageRegion,Pair<List<String>, List<String>>> calcCopyCount(Map<StorageRegion, List<String>> snFiles){
		if(snFiles == null || snFiles.isEmpty()){
			return null;
		}
		StorageRegion sn = null;
		List files = null;
		Map<String, Integer> fileCopyCount = null;
		Pair<List<String>,List<String>> result = null;
		Map<StorageRegion,Pair<List<String>, List<String>>> copyMap = new HashMap<StorageRegion,Pair<List<String>, List<String>>>();
		
		for(Map.Entry<StorageRegion, List<String>> entry : snFiles.entrySet()){
			sn = entry.getKey();
			files = entry.getValue();
			if(files == null || files.isEmpty()){
				continue;
			}
			fileCopyCount = calcFileCount(files);
			if(fileCopyCount == null || fileCopyCount.isEmpty()){
				continue;
			}
			result = filterLoser(fileCopyCount, sn.getReplicateNum());
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
	 * @return
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	public static Map<StorageRegion, List<String>> collectionSnFiles(List<Service> services, List<StorageRegion> snList,final Map<String,Long> snTimes){
		Map<StorageRegion,List<String>> snMap = new HashMap<>();
		DiskNodeClient client = null;
		int reCount = 0;
		String snName = null;
		String path = null;
		List<String> strs = null;
		long time = 0;
		String dirName = null;
		for(Service service : services){
			try {
				client = TcpClientUtils.getClient(service.getHost(), service.getPort(), service.getExtraPort(), 5000);
				long granule = 0;
				for(StorageRegion sn : snList){
					granule = Duration.parse(sn.getFilePartitionDuration()).toMillis();
					reCount = sn.getReplicateNum();
					snName = sn.getName();
					if(!snTimes.containsKey(snName)) {
						LOG.debug("<collectionSnFiles> sntime don't contain {}", snName);
						continue;
					}
					time = snTimes.get(snName);
					dirName = TimeUtils.timeInterval(time, granule);
					for(int i = 1; i <=reCount; i++){
						path = "/"+snName+"/"+i+"/"+dirName;
						LOG.info("<collectionSnFiles> path :{}",path);
						strs = getFileList(client, path);
						if(strs == null || strs.isEmpty()) {
							LOG.debug("<collectionSnFiles> files is empty {}", path);
							continue;
						}
						LOG.debug("Collection dirName :{},{} size :{}",dirName,path, strs.size());
						if(!snMap.containsKey(sn)){
							snMap.put(sn, new ArrayList<String>());
						}
						snMap.get(sn).addAll(strs);
					}
				}
			} catch (Exception e) {
				LOG.error("{}",e);
			}finally{
				if(client != null){
					try {
						client.close();
					}
					catch (IOException e) {
						LOG.error("{}",e);
					}
				}
			}
			
		}
		return clearUnLawFiles(snMap);
	}
	public static Map<StorageRegion, List<String>> clearUnLawFiles(Map<StorageRegion,List<String>> data){
		Map<StorageRegion, List<String>> rMap = new HashMap<>();
		if(data == null || data.isEmpty()) {
			return rMap;
		}
		StorageRegion sr = null;
		List<String> files = null;
		for(Map.Entry<StorageRegion, List<String>> entry : data.entrySet()) {
			sr = entry.getKey();
			files = entry.getValue();
			files = clearUnLawFiles(files);
			rMap.put(sr, files);
		}
		return rMap;
	}
	public static List<String> clearUnLawFiles(List<String> files){
		List<String> rList = new ArrayList<String>();
		if(files == null || files.isEmpty()) {
			return rList;
		}
		List<String> errors = new ArrayList<String>();
		String[] checks = null;
		for(String file : files) {
			// 排除rd文件
			if(file.indexOf(".rd") > 0){
				file = file.substring(0, file.indexOf(".rd"));
				errors.add(file);
				continue;
			}
			//排除非法数据
			checks = BrStringUtils.getSplit(file, "_");
			if(checks == null|| checks.length<=1) {
				errors.add(file);
				continue;
			}
		}
		return filterErrors(files, errors);
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
			LOG.debug("<getFileList> file size :{}",0);
			return null;
		}
		LOG.debug("<getFileList> file size :{}",files.size());
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
//		List<String> errorFiles = new ArrayList<>();
		String[] checks = null; 
		for(FileInfo file : files){
			path = file.getPath();
			fileName = getFileName(path);
			if(dirName.equals(fileName)){
				continue;
			}
//			// 排除rd文件
//			if(fileName.indexOf(".rd") > 0){
//				fileName = fileName.substring(0, fileName.indexOf(".rd"));
//				filterRd.add(fileName);
//				continue;
//			}
			// 排除非法数据
//			checks = BrStringUtils.getSplit(fileName, "_");
//			if(checks == null|| checks.length<=1) {
//				errorFiles.add(fileName);
//				continue;
//			}
			strs.add(fileName);
		}
//		return filterErrors(strs, errorFiles);
		return strs;
	}
	/**
	 * 概述：过滤rd文件
	 * @param files
	 * @param rds
	 * @return
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	public static List<String> filterErrors(final List<String> files, final List<String> rds){
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
	/***
	 * 概述：获取文件名
	 * @param path
	 * @return
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
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
	public static List<StorageRegion> filterSn(List<StorageRegion> sns, int size){
		List<StorageRegion> filters = new ArrayList<StorageRegion>();
		if(sns == null || sns.isEmpty()){
			return filters;
		}
		int count = 0;
		String snName = null;
		for(StorageRegion sn : sns){
			count = sn.getReplicateNum();
			snName = sn.getName();
			if(count == 1){
				LOG.info("<filterSn> sn {} {} skip",snName,count);
				continue;
			}
			if(count >size){
				LOG.info("<filterSn> sn {} {} {} skip",snName,count, size);
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
		Pair<List<String>,List<String>> result = new Pair<List<String>,List<String>>(filterLitterResult,filterBiggestResult);
		return result;
	}
	
	/**
	 * 概述：添加第一次出现的sn
	 * @param sourceTimes
	 * @param needSns
	 * @return
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	public static Map<String,Long> repairTime(final Map<String,Long> sourceTimes, List<StorageRegion> needSns){
		Map<String,Long> repairs = new ConcurrentHashMap<>();
		if(needSns == null || needSns.isEmpty()) {
			return repairs;
		}
		long currentTime = System.currentTimeMillis();
		String snName = null;
		long startTime = 0L;
		long sGra = 0L;
		long cGra = 0L;
		long granule = 0;
		for(StorageRegion sn : needSns) {
			granule = Duration.parse(sn.getFilePartitionDuration()).toMillis();
			cGra = currentTime - currentTime%granule;
			snName = sn.getName();
			if(sourceTimes!=null && sourceTimes.containsKey(snName)) {
				startTime = sourceTimes.get(snName);
			}else{
                startTime = sn.getCreateTime();
            }
			if(currentTime - startTime < granule) {
				continue;
			}
			sGra = startTime - startTime % granule;
			if(sGra == cGra) {
				LOG.info("skip {} create copy check task!! because forbid check current time ",snName);
				continue;
			}
			repairs.put(snName, sGra);
		}
		return repairs;
	}
}
