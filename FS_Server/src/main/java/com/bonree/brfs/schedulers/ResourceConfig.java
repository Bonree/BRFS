package com.bonree.brfs.schedulers;
/*****************************************************************************
 * 版权信息：北京博睿宏远数据科技股份有限公司
 * Copyright: Copyright (c) 2007北京博睿宏远数据科技股份有限公司,Inc.All Rights Reserved.
 * 
 * @date 2018年4月10日 下午4:26:47
 * @Author: <a href=mailto:zhucg@bonree.com>朱成岗</a>
 * @Description: 资源采集
 *****************************************************************************
 */
public class ResourceConfig {
	//公共配置
	private String zkNode;
	private String dataPath;
	private String serverId;
	private String zkUrl;
	private String ip;
	private String storageNameZkNode;
	private String resourceZkNode;
	private String baseZkNode;
	private String resouceClass;
	private String tmpSns;
	private int resourceThreadPoolSize;
	private long gatherIveralTime;
	private int collectionCount;
	private long updateResourceTime;
	private int systemDeleteThreadPoolSize;
	private int systemMergeThreadPoolSize;
	private int systemCheckThreadPoolSize;
	private int userThreadPoolSize;
	private String libs;
	public String getZkNode() {
		return zkNode;
	}
	public void setZkNode(String zkNode) {
		this.zkNode = zkNode;
	}
	public String getDataPath() {
		return dataPath;
	}
	public void setDataPath(String dataPath) {
		this.dataPath = dataPath;
	}
	public String getServerId() {
		return serverId;
	}
	public void setServerId(String serverId) {
		this.serverId = serverId;
	}
	public String getZkUrl() {
		return zkUrl;
	}
	public void setZkUrl(String zkUrl) {
		this.zkUrl = zkUrl;
	}
	public String getIp() {
		return ip;
	}
	public void setIp(String ip) {
		this.ip = ip;
	}
	public String getStorageNameZkNode() {
		return storageNameZkNode;
	}
	public void setStorageNameZkNode(String storageNameZkNode) {
		this.storageNameZkNode = storageNameZkNode;
	}
	public String getResourceZkNode() {
		return resourceZkNode;
	}
	public void setResourceZkNode(String resourceZkNode) {
		this.resourceZkNode = resourceZkNode;
	}
	public String getBaseZkNode() {
		return baseZkNode;
	}
	public void setBaseZkNode(String baseZkNode) {
		this.baseZkNode = baseZkNode;
	}
	public int getResourceThreadPoolSize() {
		return resourceThreadPoolSize;
	}
	public void setResourceThreadPoolSize(int resourceThreadPoolSize) {
		this.resourceThreadPoolSize = resourceThreadPoolSize;
	}
	public long getGatherIveralTime() {
		return gatherIveralTime;
	}
	public void setGatherIveralTime(long gatherIveralTime) {
		this.gatherIveralTime = gatherIveralTime;
	}
	public int getCollectionCount() {
		return collectionCount;
	}
	public void setCollectionCount(int collectionCount) {
		this.collectionCount = collectionCount;
	}
	public long getUpdateResourceTime() {
		return updateResourceTime;
	}
	public void setUpdateResourceTime(long updateResourceTime) {
		this.updateResourceTime = updateResourceTime;
	}
	public int getSystemDeleteThreadPoolSize() {
		return systemDeleteThreadPoolSize;
	}
	public void setSystemDeleteThreadPoolSize(int systemDeleteThreadPoolSize) {
		this.systemDeleteThreadPoolSize = systemDeleteThreadPoolSize;
	}
	public int getSystemMergeThreadPoolSize() {
		return systemMergeThreadPoolSize;
	}
	public void setSystemMergeThreadPoolSize(int systemMergeThreadPoolSize) {
		this.systemMergeThreadPoolSize = systemMergeThreadPoolSize;
	}
	public int getSystemCheckThreadPoolSize() {
		return systemCheckThreadPoolSize;
	}
	public void setSystemCheckThreadPoolSize(int systemCheckThreadPoolSize) {
		this.systemCheckThreadPoolSize = systemCheckThreadPoolSize;
	}
	public int getUserThreadPoolSize() {
		return userThreadPoolSize;
	}
	public void setUserThreadPoolSize(int userThreadPoolSize) {
		this.userThreadPoolSize = userThreadPoolSize;
	}
	public String getResouceClass() {
		return resouceClass;
	}
	public void setResouceClass(String resouceClass) {
		this.resouceClass = resouceClass;
	}
	public String getTmpSns() {
		return tmpSns;
	}
	public void setTmpSns(String tmpSns) {
		this.tmpSns = tmpSns;
	}
	public String getLibs() {
		return libs;
	}
	public void setLibs(String libs) {
		this.libs = libs;
	}
	
}
