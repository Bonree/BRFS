package com.bonree.brfs.schedulers.utils;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.bonree.brfs.common.utils.BRFSFileUtil;
import com.bonree.brfs.common.utils.BRFSPath;
import com.bonree.brfs.schedulers.ManagerContralFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bonree.brfs.common.utils.BrStringUtils;
import com.bonree.brfs.common.utils.FileUtils;
import com.bonree.brfs.common.zookeeper.curator.CuratorClient;
import com.bonree.brfs.duplication.storageregion.StorageRegion;
import com.bonree.brfs.rebalance.route.SecondIDParser;
import com.bonree.brfs.schedulers.jobs.biz.WatchSomeThingJob;
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
	private static final Logger LOG = LoggerFactory.getLogger(WatchDog.class);
	private static Queue<String> preys = new ConcurrentLinkedQueue<String>();
	private static ExecutorService executor = Executors.newFixedThreadPool(1);
	private static CuratorClient curatorClient = null;
	private static long lastTime = 0;
	private static boolean isRun = false;
	/**
	 * 概述：获取
	 * @param sim
	 * @param sns
	 * @param dataPath
	 * @param limitTime
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	public static void searchPreys(ServerIDManager sim, Collection<StorageRegion> sns,String zkHosts,String baseRoutesPath, String dataPath, long limitTime) {
		if(sns == null || sns.isEmpty() || BrStringUtils.isEmpty(dataPath)) {
			LOG.debug("skip search data because is empty");
			return;
		}
		if(isRun) {
			LOG.info("SKip search data because there is one");
			return;
		}
		lastTime = System.currentTimeMillis();
		// sn 目录及文件
		int snId;
		SecondIDParser parser;
		// 初始化zk连接
		if(curatorClient == null){
			curatorClient = ManagerContralFactory.getInstance().getClient();
		}
		Map<String,String> snMap;
		long granule;
		long snLimitTime;
		for(StorageRegion sn : sns) {
			if(WatchSomeThingJob.getState(WatchSomeThingJob.RECOVERY_STATUSE)) {
				LOG.warn("skip search data because there is one reblance");
				return;
			}
			snId = sn.getId();
			granule = Duration.parse(sn.getFilePartitionDuration()).toMillis();;
			snLimitTime = limitTime - limitTime%granule;
			LOG.info(" watch dog eat {} :{}", sn.getName(),sn.getId());

            // 单个副本的不做检查
            if(sn.getReplicateNum()<=1) {
                continue;
            }
            parser = new SecondIDParser(curatorClient, snId, baseRoutesPath);
            // 使用前必须更新路由规则，否则会解析错误
            parser.updateRoute();
            snMap = new HashMap<>();
			snMap.put(BRFSPath.STORAGEREGION,sn.getName());
            List<BRFSPath> sfiles = BRFSFileUtil.scanBRFSFiles(dataPath,snMap,snMap.size(), new BRFSDogFoodsFilter(sim,parser,sn,snLimitTime));
            if(sfiles == null || sfiles.isEmpty()){
                continue;
            }
            for(BRFSPath brfsPath : sfiles){
                preys.add(dataPath+FileUtils.FILE_SEPARATOR+brfsPath.toString());
            }

		}
		//若见采集结果不为空则调用删除线程
		if(preys.size() > 0) {
			isRun = true;
			executor.execute(new Runnable() {
				
				@Override
				public void run() {
					// 为空跳出
					if(preys == null) {
						LOG.debug("queue is empty skip !!!");
						return;
					}
					int count = 0;
					String path;
					while(!preys.isEmpty()) {
						try {
							path = preys.poll();
							boolean deleteFlag = FileUtils.deleteFile(path);
							LOG.debug("file : {} deleting!",path);
							if(!deleteFlag) {
								LOG.info("file : {} cann't delete !!!",path);
							}
							count ++;
							if(count%100 == 0) {
								Thread.sleep(1000L);
							}
						}catch (Exception e) {
							LOG.error("watch dog delete file error {}",e);
						}
					}
					isRun = false;
					
				}
			});
		}

	}
	public static long getLastTime() {
		return lastTime;
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
	private static Map<String,List<String>> collectFood(String datapath,StorageRegion sn,long limitTime, long granule){
		Map<String,List<String>> foods = new ConcurrentHashMap<String,List<String>>();
		if(sn == null || BrStringUtils.isEmpty(datapath)) {
			LOG.info("sn or dataPath is empty !!! ");
			return foods;
		}
		int copyCount = sn.getReplicateNum();
		String snName = sn.getName();
		String dirPath;
		Map<String,List<String>> part;
		for(int i = 1; i<=copyCount; i++) {
			dirPath = datapath + "/" + snName + "/" + i;
			part = FileCollection.collectLocalFiles(dirPath, limitTime, granule);
			if(part == null || part.isEmpty()) {
				LOG.debug(" part is empty !!!");
				continue;
			}
			foods.putAll(part);
		}
		return foods;
	}
}
