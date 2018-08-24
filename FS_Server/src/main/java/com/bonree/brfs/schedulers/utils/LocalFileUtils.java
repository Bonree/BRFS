package com.bonree.brfs.schedulers.utils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

import com.bonree.brfs.common.utils.BrStringUtils;
import com.bonree.brfs.common.utils.FileUtils;
import com.bonree.brfs.common.utils.Pair;
import com.bonree.brfs.common.utils.TimeUtils;

public class LocalFileUtils {
	/**
	 * 收集指定时间段的目录
	 * @param dataPath
	 * @param snName
	 * @param startTime
	 * @param endTime
	 * @return
	 */
	public  static List<String> collectDucationTimeDirs(String dataPath, String snName, long startTime, long endTime){
		List<String> parDirs = collectPartitionDirs(dataPath,snName);
		if(parDirs == null || parDirs.isEmpty()){
			return null;
		}
		return LocalFileUtils.collectTimeDirs(parDirs, startTime,endTime,1,false);
	}
	/**
	 * 收集指定时间段的目录名称
	 * @param dataPath
	 * @param snName
	 * @param startTime
	 * @param endTime
	 * @return
	 */
	public  static List<String> collectDucationTimeDirNames(String dataPath, String snName, long startTime, long endTime){
		List<String> parDirs = collectPartitionDirs(dataPath,snName);
		if(parDirs == null || parDirs.isEmpty()){
			return null;
		}
		return collectTimeDirs(parDirs, startTime,endTime,1, true);
	}
	
	/**
	 * 概述：获取分区目录
	 * @param dataPath
	 * @param snName
	 * @return
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	public static List<String> collectPartitionDirs(final String dataPath,final String snName){
		List<String> dirs = new ArrayList<String>();
		if(BrStringUtils.isEmpty(dataPath) || BrStringUtils.isEmpty(snName)) {
			return dirs;
		}
		String path = null;
		List<String> childs = FileUtils.listFileNames(coveryPath(dataPath) + "/"+snName);
		for(String child : childs){
			path = coveryPath(dataPath) + "/" +snName +"/" + child;
			if(!FileUtils.isExist(path)) {
				continue;
			}
			if(!FileUtils.isDirectory(path)) {
				continue;
			}
			dirs.add(path);
		}
		return dirs;
	}
	/**
	 * 概述：获取分区目录
	 * @param dataPath
	 * @param snName
	 * @param patitionNum
	 * @return
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	public static List<String> getPartitionDirs(final String dataPath,final String snName, final int patitionNum){
		List<String> dirs = new ArrayList<String>();
		if(BrStringUtils.isEmpty(dataPath) || BrStringUtils.isEmpty(snName) || patitionNum <= 0) {
			return dirs;
		}
		String path = null;
		
		for(int i = 1; i<=patitionNum; i++) {
			path = coveryPath(dataPath) + "/" +snName +"/" + i;
			if(!FileUtils.isExist(path)) {
				continue;
			}
			if(!FileUtils.isDirectory(path)) {
				continue;
			}
			dirs.add(path);
		}
		return dirs;
	}
	/**
	 * 概述：收集目录
	 * @param partitionDirs
	 * @param startTime
	 * @param endTime
	 * @param type 0：收集指定时间及以前的目录，1：收集指定时间的目录
	 * @param isName
	 * @return
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	public static List<String> collectTimeDirs(List<String> partitionDirs, long startTime, long endTime, int type, boolean isName){
		List<String> deleteDirs = new ArrayList<String>();
		if(partitionDirs == null || partitionDirs.isEmpty()) {
			return deleteDirs;
		}
		if(startTime >= endTime || startTime < 0 || endTime < 0) {
			return deleteDirs;
		}
		List<String> tmpList = null;
		for(String partDir : partitionDirs) {
			if(!FileUtils.isExist(partDir)) {
				continue;
			}
			if(!FileUtils.isDirectory(partDir)) {
//				deleteDirs.add(partDir);
				continue;
			}
			tmpList = collectTimeDirs(partDir, startTime, endTime, type, isName);
			if(tmpList == null || tmpList.isEmpty()) {
				continue;
			}
			deleteDirs.addAll(tmpList);
		}
		return deleteDirs;
	}
	/**
	 * 概述：收集目录
	 * @param partDir
	 * @param startTime
	 * @param endTime
	 * @param type 0：收集指定时间及以前的目录，1：收集指定时间的目录
	 * @return
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	public static List<String> collectTimeDirs(String partDir,long startTime, long endTime,int type, boolean isName){
		List<String> dirNames = FileUtils.listFileNames(partDir);
		Pair<Long,Long> times = null;
		String path = null;
		List<String> dirs = new ArrayList<String>();
		for(String dirName : dirNames) {
			times = ananlyDirNames(dirName);
			if(times == null) {
				continue;
			}
			path = partDir + "/" + dirName;
			switch(type) {
				case 0:
					if(isBefore(times, startTime, endTime)) {
						if(isName) {
							dirs.add(dirName);
						}else {
							dirs.add(path);
						}
					}
					break;
				case 1:
					if(isDucation(times, startTime, endTime)) {
						if(isName) {
							dirs.add(dirName);
						}else {
							dirs.add(path);
						}
					}
					break;
				default:
			}
		}
		return dirs;
	}
	/**
	 * 概述：之前及指定的时间
	 * @param times
	 * @param startTime
	 * @param endTime
	 * @return
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	public static boolean isBefore(Pair<Long,Long> times, long startTime, long endTime) {
		if(times == null ) {
			return false;
		}
		if(times.getSecond() <= endTime) {
			return true;
		}else {
			return false;
		}
	}
	/**
	 * 概述：指定时间段内
	 * @param times
	 * @param startTime
	 * @param endTime
	 * @return
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	public static boolean isDucation(Pair<Long,Long> times, long startTime, long endTime) {
		if(times == null ) {
			return false;
		}
		if(times.getFirst() < startTime) {
			return false;
		}
		if(times.getFirst() > endTime) {
			return false;
		}
		if(times.getSecond() > endTime) {
			return false;
		}
		return true;
	}
	/**
	 * 概述：修正目录路径
	 * @param path
	 * @return
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	public static String coveryPath(String path) {
		String paths = new String(path);
		int index = paths.lastIndexOf("/");
		if (index == path.length() - 1) {
			paths = paths.substring(0, index);
		}
		index = paths.lastIndexOf("\\");
		if (index == path.length() - 1) {
			paths = paths.substring(0, index);
		}
		return paths;
	}
	/**
	 * 概述：解析目录时间
	 * @param dirName
	 * @return
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	public static Pair<Long,Long> ananlyDirNames(String dirName){
		if(BrStringUtils.isEmpty(dirName)) {
			return null;
		}
		if(!dirName.contains("_")) {
			return null;
		}
		String[] timeStr = BrStringUtils.getSplit(dirName, "_");
		if(timeStr == null || timeStr.length != 2) {
			return null;
		}
		long start = TimeUtils.getMiles(timeStr[0]);
		long end = TimeUtils.getMiles(timeStr[1]);
		return new Pair<Long,Long>(start,end);
	}
	/**
	 * 概述：转换为时间段
	 * @param dirs
	 * @return
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	public static List<Pair<Long,Long>> converPairByUniqueness(List<String> dirs){
		Set<Pair<Long,Long>> pDirs = new HashSet<Pair<Long,Long>>();
		Pair<Long,Long> pair = null;
		if(dirs == null || dirs.isEmpty()) {
			return new ArrayList<Pair<Long,Long>>();
		}
		for(String dir : dirs) {
			pair = ananlyDirNames(dir);
			pDirs.add(pair);
		}
		return new ArrayList<Pair<Long,Long>>(pDirs);
	}
	public static List<Pair<Long,Long>> sortTime(List<Pair<Long,Long>> dirs){
		Map<Long,List<Pair<Long,Long>>> graMap = sortGranule(dirs);
		Long[] gras = orderGranule(graMap.keySet());
		List<Pair<Long,Long>> tmp = null;
		List<Pair<Long,Long>> sortList = new ArrayList<Pair<Long,Long>>();
		for(Long gra : gras) {
			tmp = graMap.remove(gra);
			if(graMap == null || graMap.isEmpty()) {
				sortList.addAll(tmp);
				continue;
			}
			for(Pair<Long,Long> pair : tmp) {
				sortList.add(pair);
				for(Map.Entry<Long, List<Pair<Long,Long>>> entry : graMap.entrySet()) {
					if(entry.getValue() == null || entry.getValue().isEmpty()) {
						graMap.remove(entry.getKey());
						continue;
					}
					for(Pair<Long,Long> sPair : entry.getValue()) {
						if(pair.getFirst() <= sPair.getFirst() && pair.getSecond() >= sPair.getSecond()) {
							graMap.get(entry.getKey()).remove(sPair);
						}
					}
					
				}
			}
		}
		return sortList;
	}
	
	/**
	 * 概述：分拣粒度
	 * @param dirs
	 * @return
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	public static Map<Long,List<Pair<Long,Long>>> sortGranule(List<Pair<Long,Long>> dirs){
		Map<Long,List<Pair<Long,Long>>> sortMap = new ConcurrentHashMap<Long,List<Pair<Long,Long>>>();
		long granule = 0l;
		for(Pair<Long,Long> dirGra : dirs) {
			granule = dirGra.getSecond() - dirGra.getFirst();
			if(!sortMap.containsKey(granule)) {
				sortMap.put(granule, new CopyOnWriteArrayList<Pair<Long,Long>>());
			}
			// 过滤重复的
			if(sortMap.get(granule).contains(dirGra)) {
				continue;
			}
			sortMap.get(granule).add(dirGra);
		}
		return sortMap;
	}
	/**
	 * 概述：粒度时间从大到小排序
	 * @param granules
	 * @return
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	public static Long[] orderGranule(final Collection<Long> granules) {
		List<Long> arrays = new ArrayList<Long>(granules);
		Collections.sort(arrays, new Comparator<Long>() {
			@Override
			public int compare(Long o1, Long o2) {
				if(o1 > o2) {
					return -1;
				}else if(o1 == o2) {
					return 0;
				}else {
					return 1;
				}
			}
		});
		return  arrays.toArray(new Long[0]);
	}
}
