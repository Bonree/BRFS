package com.bonree.brfs.schedulers.jobs.biz;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.bonree.brfs.common.utils.BrStringUtils;
import com.bonree.brfs.common.utils.FileUtils;
import com.bonree.brfs.common.zookeeper.curator.CuratorClient;
import com.bonree.brfs.duplication.storagename.StorageNameNode;
import com.bonree.brfs.rebalance.route.SecondIDParser;
import com.bonree.brfs.server.identification.ServerIDManager;

/*****************************************************************************
 * 版权信息：北京博睿宏远数据科技股份有限公司
 * Copyright: Copyright (c) 2007北京博睿宏远数据科技股份有限公司,Inc.All Rights Reserved.
 * 
 * @date 2018年6月20日 下午4:51:04
 * @Author: <a href=mailto:zhucg@bonree.com>朱成岗</a>
 * @Description: 负责清理不必要的文件
 *****************************************************************************
 */
public class WatchDog{
	private static Queue<String> preys = new ConcurrentLinkedQueue<String>();
	private static ExecutorService executor = Executors.newSingleThreadExecutor(); 
	/**
	 * 概述：获取
	 * @param sim
	 * @param parser
	 * @param sns
	 * @param dataPath
	 * @param limitTime
	 * @param granule
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	public static void searchPreys(ServerIDManager sim, Collection<StorageNameNode> sns,String zkHosts,String baseRoutesPath, String dataPath, long limitTime, long granule) {
		if(sns == null || sns.isEmpty() || BrStringUtils.isEmpty(dataPath)) {
			return;
		}
		// sn 目录及文件
		Map<String,List<String>> files = null;
		List<String> partPreys = null;
		int snId = -1;
		SecondIDParser parser = null;
		CuratorClient curatorClient = CuratorClient.getClientInstance(zkHosts);
		for(StorageNameNode sn : sns) {
			parser = new SecondIDParser(curatorClient, snId, baseRoutesPath);
			// 单个副本的不做检查
			if(sn.getReplicateCount()<=1) {
				continue;
			}
			snId = sn.getId();
			// 收集sn文件信息
			files = collectFood(dataPath, sn, limitTime, granule);
			// 找到多余的文件 猎物
			partPreys = FileCollection.crimeFiles(files, snId, sim,parser);
			if(partPreys ==null || partPreys.isEmpty()) {
				continue;
			}
			preys.addAll(partPreys);
		}
		//若见采集结果不为空则调用删除线程
		if(preys.size() > 0) {	
			executor.execute(new Runnable() {
				
				@Override
				public void run() {
					// 为空跳出
					if(preys == null) {
						return;
					}
					int count = 0;
					String path = null;
					while(!preys.isEmpty()) {
						try {
							path = preys.poll();
							FileUtils.deleteFile(path);
							count ++;
							if(count%100 == 0) {
								Thread.sleep(1000l);
							}
						}catch (Exception e) {
							e.printStackTrace();
						}
					}
					
				}
			});
		}
		//关闭zookeeper连接
		curatorClient.close();
	}
	
	public static void abandonFoods() {
		preys.clear();
	}
	/**
	 * 概述：采集文件
	 * @param datapath
	 * @param sn
	 * @param limitTime
	 * @param granule
	 * @return
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	private static Map<String,List<String>> collectFood(String datapath,StorageNameNode sn,long limitTime, long granule){
		Map<String,List<String>> foods = new ConcurrentHashMap<String,List<String>>();
		if(sn == null || BrStringUtils.isEmpty(datapath)) {
			return foods;
		}
		int copyCount = sn.getReplicateCount();
		String snName = sn.getName();
		String dirPath = null;
		Map<String,List<String>> part = null;
		for(int i = 1; i<=copyCount; i++) {
			dirPath = datapath + "/" + snName + "/" + i;
			part = FileCollection.collFiles(dirPath, limitTime, granule);
			if(part == null || part.isEmpty()) {
				continue;
			}
			foods.putAll(part);
		}
		return foods;
	}	
}
