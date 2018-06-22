package com.bonree.brfs.schedulers.jobs.biz;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bonree.brfs.common.utils.BrStringUtils;
import com.bonree.brfs.common.utils.FileUtils;
import com.bonree.brfs.common.utils.TimeUtils;
import com.bonree.brfs.duplication.storagename.StorageNameNode;
import com.bonree.brfs.rebalance.route.SecondIDParser;
import com.bonree.brfs.server.identification.ServerIDManager;
/******************************************************************************
 * 版权信息：北京博睿宏远数据科技股份有限公司
 * Copyright: Copyright (c) 2007北京博睿宏远数据科技股份有限公司,Inc.All Rights Reserved.
 * 
 * @date 2018年6月20日 上午9:42:57
 * @Author: <a href=mailto:zhucg@bonree.com>朱成岗</a>
 * @Description: 文件收集
 *****************************************************************************
 */
public class FileCollection {
	private static final Logger LOG = LoggerFactory.getLogger("FileCollection");
	/**
	 * 概述：根据路径收集文件名
	 * @param path
	 * @return
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	public static Map<String,List<String>> collFiles(String path, long limitTime, long granule){
		if(!FileUtils.isExist(path)) {
			LOG.debug("<collFiles> file path is not exists {}",path);
			return null;
		}
		if(!FileUtils.isDirectory(path)) {
			LOG.debug("<collFiles> file path is not directory {}",path);
			return null;
		}
		String limitStr = limitTime <= 0 ? "END" : TimeUtils.timeInterval(limitTime, granule);
		List<String> dirs = FileUtils.listFileNames(path);
		//升序排列任务
		
		Collections.sort(dirs, new Comparator<String>() {
			public int compare(String o1, String o2) {
				return o1.compareTo(o2);
			}
		});
		Map<String,List<String>> dirFiles = new ConcurrentHashMap<String,List<String>>();
		List<String> parts = null;
		String tmpPath = null;
		for(String dir : dirs) {
			if(dir.compareTo(limitStr) >= 0) {
				continue;
			}
			tmpPath = path + "/" + dir;
			if(!FileUtils.isExist(tmpPath)) {
				continue;
			}
			if(!FileUtils.isDirectory(tmpPath)) {
				continue;
			}
			parts = FileUtils.listFileNames(tmpPath);
			if(parts == null || parts.isEmpty()) {
				continue;
			}
			dirFiles.put(tmpPath, parts);
		}
		return dirFiles;
	}
	/**
	 * 概述：
	 * @param files
	 * @param sn
	 * @param sim
	 * @param parser
	 * @return
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	public static List<String> crimeFiles(Map<String,List<String>> dirFiles,int snId, ServerIDManager sim,SecondIDParser parser){
		List<String> crimers = new ArrayList<String>();
		String secondLocalId = sim.getSecondServerID(snId);
		if(BrStringUtils.isEmpty(secondLocalId)) {
			return null;
		}
		List<String> secondIds = null;
		List<String> files = null;
		String path = null;
		for(Map.Entry<String, List<String>> entry : dirFiles.entrySet()) {
			path = entry.getKey();
			files = entry.getValue();
			// 无文件，空目录 不检测
			if(BrStringUtils.isEmpty(path)|| files == null ||files.isEmpty()) {
				continue;
			}
			for(String file : files) {
				// 1.解析文件名对应目前的二级serverId
				secondIds = analyseServices(file, parser);
				if(crimeFile(secondIds, secondLocalId)) {
					crimers.add(path + "/" +file);
				}
			}
		}
		return crimers;
	}
	/**
	 * 概述：判断单个文件是否非法
	 * @param aliveSnIds
	 * @param secondLocalId
	 * @return
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	public static boolean crimeFile(List<String> aliveSnIds, String secondLocalId) {
		if(aliveSnIds == null || aliveSnIds.isEmpty() || BrStringUtils.isEmpty(secondLocalId)) {
			return true;
		}
		return !aliveSnIds.contains(secondLocalId);		
	}
	/**
	 * 概述：解析文件名汇总的serverId
	 * @param fileName
	 * @return
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	public static List<String> analyseServices(String fileName, SecondIDParser parser) {
		List<String> snIds = new ArrayList<String>();
		if(BrStringUtils.isEmpty(fileName)) {
			return snIds;
		}
		String[] tmps = parser.getAliveSecondID(fileName);
		if(tmps == null || tmps.length <= 1) {
			return snIds;
		}
		for(int i = 1 ; i < tmps.length; i++) {
			snIds.add(tmps[i]);
		}
		return snIds;
	}
}
