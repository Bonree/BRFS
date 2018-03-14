package com.bonree.brfs.resourceschedule.commons;

import static org.junit.Assert.*;

import java.io.File;
import java.util.UUID;

import org.junit.Test;

import com.bonree.brfs.resourceschedule.utils.StringUtils;


public class CommonsTest {
	/**
	 * 概述：测试filterIP方法
	 * 
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	@Test
	public void testfilterIP(){
		String[] errorIps = {null,
				"",
				"0.0.0.0",
				"1234567890123456",
				"255.255.255.256",
				"127.0.0.1",
				"11.11",
				"255. 255.255.255",
				"255. 0 . 00.1",
				"127.1.1. 0",
				"a.12.12.12",
				"--.00.00.00",
				"00.00.00.00",
				"012.01.01.02"
				};
		for(String ip : errorIps){
			assertEquals(ip,true, Commons.filterIp(ip));
		}
	}
	@Test
	public void testMountFile(){
		String[] errorFile = {
			null,
			"",
			"  ",
			"_",
			"2343241234*&^%$$#"
		};
		for(String file : errorFile){
			assertEquals(file, true,Commons.filterMountPoint(file));
		}
	}
	/**
	 * 概述：测试Loadproperty
	 * 
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */

	@Test
	public void testLoadproperty() {
		String configPath = null;
		// 1.检查输入null是否抛出异常
		try {
			if(StringUtils.isEmpty(configPath)){
				Commons.loadLibraryPath(configPath);
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
				Commons.loadLibraryPath(configPath);
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
			Commons.loadLibraryPath(configPath);
			String afterPath = System.getProperty("java.library.path");
			if(!afterPath.contains(configPath)){
				fail("path not contains " + configPath + "; java.library.path="+afterPath);
			} 
		}
		catch (Exception e) {
			fail(e.getMessage());
		}
	}

	public static boolean  initLibrary() throws Exception{
		String path = ClassLoader.getSystemResource(".").getPath();
		String tmpPath = path.substring(0, path.lastIndexOf("FS_ResouceSchedule"));
		File file = new File(tmpPath+File.separator+"lib");
		if(!file.exists()){
			System.out.println("config java.library.path error !!! path is file");
			return false;
		}
		System.out.println("add the content to java.library.path is \"" +file.getAbsolutePath()+"\"");
		Commons.loadLibraryPath(file.getAbsolutePath());
		return true;
	}

}
