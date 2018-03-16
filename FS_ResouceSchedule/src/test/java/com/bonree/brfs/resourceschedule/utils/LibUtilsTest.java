package com.bonree.brfs.resourceschedule.utils;

import static org.junit.Assert.*;

import java.io.File;
import java.util.UUID;

import org.junit.Test;

import com.bonree.brfs.resourceschedule.commons.GatherResource;

public class LibUtilsTest {
	public static boolean  initLibrary() throws Exception{
		String path = ClassLoader.getSystemResource(".").getPath();
		String tmpPath = path.substring(0, path.lastIndexOf("FS_ResouceSchedule"));
		File file = new File(tmpPath+File.separator+"lib");
		if(!file.exists()){
			System.out.println("config java.library.path error !!! path is file");
			return false;
		}
		System.out.println("add the content to java.library.path is \"" +file.getAbsolutePath()+"\"");
		LibUtils.loadLibraryPath(file.getAbsolutePath());
		return true;
	}

	@Test
	public void test() {
		String configPath = null;
		// 1.检查输入null是否抛出异常
		try {
			if(StringUtils.isEmpty(configPath)){
				LibUtils.loadLibraryPath(configPath);
				fail("lib path is null but no Exception");
			} else{
				System.out.println("Test skip lib path is empty Test");
			}
		} catch (Exception e) {
			System.out.println("Test happen Exception :" + e.getMessage());
		}
		// 2.检查path不存在时是否抛异常		
		configPath = "C:/"+UUID.randomUUID().toString();
		try {
			File file = new File(configPath);
			if(!file.exists()){
				LibUtils.loadLibraryPath(configPath);
				fail("lib path is not exists but no Exception");
			} else {
				System.out.println("Test skip lib path is not exists Test");
			}
		}
		catch (Exception e) {
			System.out.println("Test happen Exception :" + e.getMessage());
		}
		// 3.检查若path是否生效
		configPath = "C:/";
		try {
			LibUtils.loadLibraryPath(configPath);
			String afterPath = System.getProperty("java.library.path");
			if(!afterPath.contains(configPath)){
				fail("path not contains " + configPath + "; java.library.path="+afterPath);
			} 
		}
		catch (Exception e) {
			fail(e.getMessage());
		}
	}

}
