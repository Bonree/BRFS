package com.bonree.brfs.identification.impl;

import com.bonree.brfs.common.process.LifeCycle;
import com.bonree.brfs.common.service.Service;
import com.bonree.brfs.identification.LocalPartitionInterface;
import com.bonree.brfs.partition.LocalPartitionCache;
import com.bonree.brfs.partition.PartitionCheckingRoutine;
import com.bonree.brfs.partition.PartitionGather;
import com.bonree.brfs.partition.PartitionInfoRegister;
import com.bonree.brfs.partition.model.LocalPartitionInfo;
import com.google.inject.Inject;
import org.apache.curator.framework.CuratorFramework;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;

/*******************************************************************************
 * 版权信息： 北京博睿宏远数据科技股份有限公司
 * Copyright (c) 2007-2020 北京博睿宏远数据科技股份有限公司，Inc. All Rights Reserved.
 * @date 2020年03月27日 15:21:25
 * @author: <a href=mailto:zhucg@bonree.com>朱成岗</a>
 * @description:
 ******************************************************************************/
public class DiskDaemon implements LifeCycle, LocalPartitionInterface {
	private static final Logger LOG = LoggerFactory.getLogger(DiskDaemon.class);

	private PartitionGather gather;
	private Collection<LocalPartitionInfo> partitions;
	private LocalPartitionCache cache = null;
	@Inject
	public DiskDaemon(PartitionGather gather, Collection<LocalPartitionInfo> partitions) {
		this.gather = gather;
		this.partitions = partitions;
		this.cache = new LocalPartitionCache(partitions);
	}

	@Override
	public void start() throws Exception {
		this.gather.start();
	}

	@Override
	public void stop() throws Exception {
		this.gather.stop();
	}

	@Override
	public String getDataPaths(String partitionId) {
		return this.cache.getDataPaths(partitionId);
	}

	@Override
	public String getPartitionId(String dataPath) {
		return this.cache.getPartitionId(dataPath);
	}

	@Override
	public Collection<String> listPartitionId() {
		return this.cache.listPartitionId();
	}

	public static class Builder{
		/**
		 * curator framework
		 */
		private CuratorFramework client;
		/**
		 * 本地service
		 */
		private Service localService;
		/**
		 * 分区id生成的路径
		 */
		private String partitionSeqPath;
		/**
		 * 分区存储节点的基础路径
		 */
		private String partitionGroupBasePath;
		/**
		 * 分区分组名称
		 */
		private String partitonGroup;
		/**
		 * datanode 存储数据的目录
		 */
		private String rootPath;
		/**
		 * datanode 磁盘内部文件存储路径
		 */
		private String innerPath;
		/**
		 * 采集线程的检查周期 单位 s
		 */
		private int intervalTimes;

		public Builder() {
		}

		public Builder setClient(CuratorFramework client) {
			this.client = client;
			return this;
		}

		public Builder setLocalService(Service localService) {
			this.localService = localService;
			return this;
		}

		public Builder setPartitionSeqPath(String partitionSeqPath) {
			this.partitionSeqPath = partitionSeqPath;
			return this;
		}

		public Builder setPartitionGroupBasePath(String partitionGroupBasePath) {
			this.partitionGroupBasePath = partitionGroupBasePath;
			return this;
		}

		public Builder setPartitonGroup(String partitonGroup) {
			this.partitonGroup = partitonGroup;
			return this;
		}

		public Builder setRootPath(String rootPath) {
			this.rootPath = rootPath;
			return this;
		}

		public Builder setInnerPath(String innerPath) {
			this.innerPath = innerPath;
			return this;
		}

		public Builder setIntervalTimes(int intervalTimes) {
			this.intervalTimes = intervalTimes;
			return this;
		}

		public DiskDaemon build()throws Exception{
			// 1.生成注册id实例
			DiskNodeIDImpl diskNodeID = new DiskNodeIDImpl(client,partitionSeqPath);
			// 2.生成磁盘分区id检查类
			PartitionCheckingRoutine routine = new PartitionCheckingRoutine(diskNodeID,this.rootPath,this.innerPath,this.partitonGroup);
			Collection<LocalPartitionInfo> parts = routine.checkVaildPartition();
			// 3.生成注册管理实例
			PartitionInfoRegister register = new PartitionInfoRegister(client,partitionGroupBasePath);
			// 4.生成采集线程池
			PartitionGather gather = new PartitionGather(register,localService,routine.checkVaildPartition(),this.intervalTimes);
			return new DiskDaemon(gather,parts);
		}
	}
}
