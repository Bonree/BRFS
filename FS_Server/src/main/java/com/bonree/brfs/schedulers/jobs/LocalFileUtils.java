package com.bonree.brfs.schedulers.jobs;

import java.util.ArrayList;
import java.util.List;

import com.bonree.brfs.common.utils.BrStringUtils;
import com.bonree.brfs.common.utils.FileUtils;
import com.bonree.brfs.common.utils.Pair;
import com.bonree.brfs.common.utils.TimeUtils;

public class LocalFileUtils {
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
	 * @return
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	public static List<String> collectTimeDirs(List<String> partitionDirs, long startTime, long endTime, int type){
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
				deleteDirs.add(partDir);
				continue;
			}
			tmpList = collectTimeDirs(partDir, startTime, endTime, type);
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
	public static List<String> collectTimeDirs(String partDir,long startTime, long endTime,int type){
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
						dirs.add(path);
					}
					break;
				case 1:
					if(isDucation(times, startTime, endTime)) {
						dirs.add(path);
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
}
