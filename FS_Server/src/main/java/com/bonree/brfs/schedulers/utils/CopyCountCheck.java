package com.bonree.brfs.schedulers.utils;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.bonree.brfs.rebalance.route.impl.RouteParser;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bonree.brfs.common.service.Service;
import com.bonree.brfs.common.utils.BrStringUtils;
import com.bonree.brfs.common.utils.Pair;
import com.bonree.brfs.common.utils.TimeUtils;
import com.bonree.brfs.common.zookeeper.curator.CuratorClient;
import com.bonree.brfs.disknode.client.DiskNodeClient;
import com.bonree.brfs.disknode.server.handler.data.FileInfo;
import com.bonree.brfs.duplication.storageregion.StorageRegion;
import com.bonree.brfs.email.EmailPool;
import com.bonree.brfs.schedulers.ManagerContralFactory;
import com.bonree.brfs.server.identification.ServerIDManager;
import com.bonree.mail.worker.MailWorker;
import com.fasterxml.jackson.databind.ObjectMapper;

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
	private static final Logger LOG = LoggerFactory.getLogger(CopyCountCheck.class);
	/***
	 * 概述：获取文件缺失的sn
	 * @param storageNames
	 * @param services
	 * @param snTimes
	 * @return
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	public static Map<String,List<String>> collectLossFile(List<StorageRegion> storageNames, List<Service> services, Map<String,Long> snTimes) throws Exception{
		Map<StorageRegion, List<String>> snFiles = collectionSnFiles(services, storageNames,snTimes);
		if(snFiles == null|| snFiles.isEmpty()) {
			LOG.info("system no data !!!");
			return null;
		}
		Map<StorageRegion,Pair<List<String>, List<String>>> copyMap = calcCopyCount(snFiles);
		if(copyMap == null|| copyMap.isEmpty()){
			LOG.info("cluster data is normal !!!");
			return null;
		}
		return lossFiles(copyMap);
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
		StorageRegion sn;
		String snName;
		Pair<List<String>,List<String>> cache;
		List<String> losss;
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
		StorageRegion sn;
		List files;
		Map fileCopyCount;
		Pair<List<String>,List<String>> result;
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
	public static Map<StorageRegion, List<String>> collectionSnFiles(List<Service> services, List<StorageRegion> snList,final Map<String,Long> snTimes)throws Exception{
		Map<StorageRegion,List<String>> snMap = new HashMap<>();
		DiskNodeClient client = null;
		int reCount;
		String snName = null;
		String path;
		List<String> strs;
		long time;
		String dirName;
        String sid;
        ManagerContralFactory mcf = ManagerContralFactory.getInstance();
        ServerIDManager sim = mcf.getSim();
        CuratorClient zkClient = mcf.getClient();
		RouteParser parser;
        String basePath = mcf.getZkPath().getBaseRoutePath();
        int timeout = 10000;
		for(Service service : services){
            try{
				client = TcpClientUtils.getClient(service.getHost(), service.getPort(), service.getExtraPort(), timeout);
				long granule;
				for(StorageRegion sn : snList){

				    parser = new RouteParser(sn.getId(),mcf.getRouteLoader());

				    sid = sim.getOtherSecondID(service.getServiceId(),sn.getId());
					granule = Duration.parse(sn.getFilePartitionDuration()).toMillis();
					reCount = sn.getReplicateNum();
					snName = sn.getName();
					if(!snTimes.containsKey(snName)) {
						LOG.debug("sntime don't contain {}", snName);
						continue;
					}
					time = snTimes.get(snName);
					dirName = TimeUtils.timeInterval(time, granule);
					LOG.info("[TEST 2 copyTaskCreator ] sn: {} time: {} path:{}",snName,TimeUtils.formatTimeStamp(time),dirName);
					for(int i = 1; i <=reCount; i++){
						path = "/"+snName+"/"+i+"/"+dirName;
						LOG.debug("path :{}",path);
						strs = getFileList(parser, client, path, sid);
						if(strs == null || strs.isEmpty()) {
							LOG.debug("files is empty {}", path);
							continue;
						}
						LOG.info("Collection dirName :{},{} size :{}",dirName,path, strs.size());
						if(!snMap.containsKey(sn)){
							snMap.put(sn, new ArrayList<>());
						}
						snMap.get(sn).addAll(strs);
					}
				}
            }catch(Exception e){
				EmailPool emailPool = EmailPool.getInstance();
				MailWorker.Builder builder = MailWorker.newBuilder(emailPool.getProgramInfo());
				builder.setModel("collect file execute 模块服务发生问题");
				builder.setException(e);
				builder.setMessage(mcf.getGroupName()+"("+mcf.getServerId()+")服务 执行任务时发生问题");
				Map<String,String> map = new HashMap<>();
				map.put("remote ",service.getHost());
				map.put("connectTimeout",String.valueOf(timeout));
				map.put("sn", StringUtils.isEmpty(snName) ? "" :snName);
				if(snTimes != null && !snTimes.isEmpty()){
				    ObjectMapper objectMapper = new ObjectMapper();
					map.put("sntime", objectMapper.writeValueAsString(snTimes));
				}
				builder.setVariable(map);
				emailPool.sendEmail(builder);
				throw  e;
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
		StorageRegion sr;
		List<String> files;
		for(Map.Entry<StorageRegion, List<String>> entry : data.entrySet()) {
			sr = entry.getKey();
			files = entry.getValue();
			files = clearUnLawFiles(files);
			rMap.put(sr, files);
		}
		return rMap;
	}
	public static List<String> clearUnLawFiles(List<String> files){
		List<String> rList = new ArrayList<>();
		if(files == null || files.isEmpty()) {
			return rList;
		}
		List<String> errors = new ArrayList<>();
		String[] checks;
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
	public static List<String> getFileList(RouteParser parser, DiskNodeClient client,  String path, String sid)throws Exception{
		if(client == null ) {
		    throw new NullPointerException("disk client is null !!!");
		}
        if(BrStringUtils.isEmpty(path)) {
            throw new NullPointerException("path is null !!!");
        }
		List<FileInfo> files =client.listFiles(path, 1);
		if(files == null || files.isEmpty()) {
			LOG.debug("path : [{}] is not data", path);
			return null;
		}
		return converToStringList(parser, files, path, sid);
	}
	 /**
	 * 概述：转换集合为str集合
	 * @param files
	 * @return
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	public static List<String> converToStringList(RouteParser parser,List<FileInfo> files,String dir, String sid){
		List<String> strs = new ArrayList<>();
		String path;
		String fileName;
		List<String> errorFiles = new ArrayList<>();
		String[] checks;
		for(FileInfo file : files){
		    if(file.getType() == FileInfo.TYPE_DIR){
		        continue;
            }
			path = file.getPath();
			fileName = getFileName(path);
//
			// 排除rd文件
			if(fileName.indexOf(".rd") > 0){
				fileName = fileName.substring(0, fileName.indexOf(".rd"));
                errorFiles.add(fileName);
                LOG.warn("file: [{}] contain rd file !! skip ",fileName);
				continue;
			}
			// 排除非法数据
			checks = BrStringUtils.getSplit(fileName, "_");
			if(checks == null|| checks.length<=1) {
				errorFiles.add(fileName);
                LOG.warn("file: [{}] is unlaw file !! skip ",fileName);
				continue;
			}
			if(isUnlaw(sid, parser, fileName)){
			    LOG.warn("file: [{}] is not [{}] file", fileName, sid);
			    continue;
            }
			strs.add(fileName);
		}
		return filterErrors(strs, errorFiles);
	}
	public static boolean isUnlaw(String sid, RouteParser parser, String fileName){
	    String[] alives = parser.searchVaildIds(fileName);
	    if(alives == null || alives.length == 0){
	        LOG.warn("[{}] analys service error !! alives is null !!!", fileName);
	        return true;
        }
        List<String> eles = Arrays.asList(alives);
	    boolean status = !eles.contains(sid);
	    if(status){
            LOG.warn("file: [{}], server: [{}], serverlist :{}",fileName,sid,eles);
        }
        return status;
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
		int lastIndex;
		lastIndex = path.lastIndexOf("/");
		if(lastIndex <0){
			lastIndex = path.lastIndexOf("\\");
		}
		if(lastIndex <0){
			return path;
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
		int count;
		String snName;
		for(StorageRegion sn : sns){
			count = sn.getReplicateNum();
			snName = sn.getName();
			if(count == 1){
				LOG.debug("sn {} {} skip",snName,count);
				continue;
			}
			if(count >size){
				LOG.debug("sn {} {} {} skip",snName,count, size);
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
		List<String> filterBiggestResult = new ArrayList<>();
		List<String> filterLitterResult = new ArrayList<>();
		String key;
		int count;
		for(Map.Entry<String, Integer> entry : resultMap.entrySet()){
			count = entry.getValue();
			key = entry.getKey();
			if(filterValue == count){
			   continue;
			}
			if(filterValue > count){
				filterLitterResult.add(key);
			}else {
				filterBiggestResult.add(key);
			}
		}
		return new Pair<>(filterLitterResult,filterBiggestResult);
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
		String snName;
		long startTime;
		long sGra;
		long cGra;
		long granule;
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
			LOG.info("[TEST 1 copyTaskCreator ] currentTime {},sn: {}, checkTime: {}",TimeUtils.formatTimeStamp(currentTime),snName,TimeUtils.formatTimeStamp(sGra));
			repairs.put(snName, sGra);
		}
		return repairs;
	}
}
