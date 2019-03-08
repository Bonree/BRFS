package com.bonree.brfs.schedulers.utils;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.zip.CRC32;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bonree.brfs.common.utils.BrStringUtils;
import com.bonree.brfs.common.utils.FileUtils;
import com.bonree.brfs.common.utils.TimeUtils;
import com.bonree.brfs.common.write.data.FSCode;
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
	private static final Logger LOG = LoggerFactory.getLogger(FileCollection.class);
	/**
	 * 概述：根据路径收集文件名
	 * @param path
	 * @return
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	public static Map<String,List<String>> collectLocalFiles(String path, long limitTime, long granule){
		if(!FileUtils.isExist(path)) {
			LOG.debug("file path is not exists {}",path);
			return null;
		}
		if(!FileUtils.isDirectory(path)) {
			LOG.debug("file path is not directory {}",path);
			return null;
		}
		String limitStr = limitTime <= 0 ? "END" : TimeUtils.timeInterval(limitTime, granule);
		List<String> dirs = FileUtils.listFileNames(path);
		//升序排列任务
		
		dirs.sort(Comparator.naturalOrder());
		Map<String,List<String>> dirFiles = new ConcurrentHashMap<>();
		List<String> parts;
		String tmpPath;
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
			parts = filterUnlaw(parts);
			if(parts == null || parts.isEmpty()) {
				continue;
			}
			dirFiles.put(tmpPath, parts);
		}
		return dirFiles;
	}

//	/**
//	 * 概述：判断单个文件是否非法
//	 * @param aliveSnIds
//	 * @param secondLocalId
//	 * @return
//	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
//	 */
//	public static boolean crimeFile(List<String> aliveSnIds, String secondLocalId) {
//		if(aliveSnIds == null || aliveSnIds.isEmpty() || BrStringUtils.isEmpty(secondLocalId)) {
//			return true;
//		}
//		return !aliveSnIds.contains(secondLocalId);
//	}
//	/**
//	 * 概述：解析文件名汇总的serverId
//	 * @param fileName
//	 * @return
//	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
//	 */
//	public static List<String> analyseServices(String fileName, SecondIDParser parser) {
//		List<String> snIds = new ArrayList<String>();
//		if(BrStringUtils.isEmpty(fileName)) {
//			return snIds;
//		}
//		String[] tmps = parser.getAliveSecondID(fileName);
//		if(tmps == null || tmps.length <= 1) {
//			return snIds;
//		}
//		for(int i = 0 ; i < tmps.length; i++) {
//			snIds.add(tmps[i]);
//		}
//		return snIds;
//	}
	/***
	 * 概述：过滤rd文件
	 * @param part
	 * @return
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	private static List<String> filterUnlaw(final List<String> part){
		List<String> filters = new ArrayList<>();
		if(part == null || part.isEmpty()) {
			return null;
		}
		String fileName = null;
		String checks[];
		for(String file : part) {
			if(file.indexOf(".rd") >= 0) {
				fileName = file.substring(0, file.indexOf(".rd"));
				filters.add(fileName);
				continue;
			}
			checks = BrStringUtils.getSplit(file, "_");
			if(checks == null|| checks.length <=1) {
				filters.add(fileName);
			}
		}
		return CopyCountCheck.filterErrors(part, filters);
	}

	/**
	 * 概述：检查单个文件
	 * @param path
	 * @return
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	public static boolean check(String path) {
		if(BrStringUtils.isEmpty(path)) {
			return false;
		}
		File file = new File(path);
		return check(file);
	}
	/**
	 * 概述：校验文件crc
	 * @param file
	 * @return
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	public static boolean check(File file) {
		RandomAccessFile raf = null;
		MappedByteBuffer buffer;
		String fileName = file.getName();
		try {
			if (!file.exists()) {
				LOG.warn("{}: not found!!", fileName);
				return false;
			}
			raf = new RandomAccessFile(file, "r");
			if(raf.length() <=0) {
				LOG.warn("{} : is empty",fileName);
				return false;
			}
			if (raf.readUnsignedByte() != 172) {
				LOG.warn("{}: Header byte is error!", fileName);
				return false;
			}
			if (raf.readUnsignedByte() != 0) {
				LOG.warn("{}: Header version is error!", fileName);
				return false;
			}
			CRC32 crc = new CRC32();
			raf.seek(0L);
			long size = raf.length() - 9L - 2L;

			if (size <= 0L) {
				LOG.warn("{}: No Content", fileName);
				return false;
			}
			buffer = raf.getChannel().map(FileChannel.MapMode.READ_ONLY, 2L, size);
			crc.update(buffer);
			raf.seek(raf.length() - 9L);
			byte[] crcBytes = new byte[8];
			int crcLen = raf.read(crcBytes);
			if(crcLen <=0){
				LOG.warn("{}: Tailer CRC is empty !!", fileName);
			}
			LOG.debug("calc crc32 code :{}, save crc32 code :{}", crc.getValue(), FSCode.byteToLong(crcBytes));
			if (FSCode.byteToLong(crcBytes) != crc.getValue()) {
				LOG.warn("{}: Tailer CRC is error!", fileName);
				return false;
			}
			if (raf.readUnsignedByte() != 218) {
				LOG.warn("{}: Tailer byte is error!", fileName);
				return false;
			}
			return true;
		}
		catch (Exception e) {
			LOG.error("check error {}:{}",fileName,e);
		}finally {
			if (raf != null) {
				try {
					raf.close();
				}
				catch (IOException e) {
					LOG.error("close {}:{}",fileName,e);
				}
			}
		}
		return false;
	}
	
}
