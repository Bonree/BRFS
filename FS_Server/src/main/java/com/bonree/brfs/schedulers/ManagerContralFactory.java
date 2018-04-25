package com.bonree.brfs.schedulers;

import com.bonree.brfs.common.service.ServiceManager;
import com.bonree.brfs.duplication.storagename.StorageNameManager;
import com.bonree.brfs.resourceschedule.service.AvailableServerInterface;
import com.bonree.brfs.schedulers.task.manager.MetaTaskManagerInterface;
import com.bonree.brfs.schedulers.task.manager.SchedulerManagerInterface;

public class ManagerContralFactory {
	/**
	 * serverId管理服务
	 */
	private ServiceManager sm;
	/**
	 * storageName管理服务
	 */
	private StorageNameManager snm;
	/**
	 * 任务发布服务
	 */
	private MetaTaskManagerInterface tm;
	/**
	 * 可用server服务
	 */
	private AvailableServerInterface asm;
	/**
	 * 任务执行服务
	 */
	private SchedulerManagerInterface stm;
	String serverId;
	String groupName;
	private ManagerContralFactory(){
	}
	private static class SimpleInstance{
		public static  ManagerContralFactory instance = new ManagerContralFactory();
	}
	public static ManagerContralFactory getInstance(){
		return SimpleInstance.instance;
	}
	public boolean isEmpty(){
		if(this.sm == null || this.snm == null){
			return true;
		}
		return false;
	}
	public ServiceManager getSm() {
		return sm;
	}
	public void setSm(ServiceManager sm) {
		this.sm = sm;
	}
	public StorageNameManager getSnm() {
		return snm;
	}
	public void setSnm(StorageNameManager snm) {
		this.snm = snm;
	}
	public MetaTaskManagerInterface getTm() {
		return tm;
	}
	public void setTm(MetaTaskManagerInterface tm) {
		this.tm = tm;
	}
	public AvailableServerInterface getAsm() {
		return asm;
	}
	public void setAsm(AvailableServerInterface asm) {
		this.asm = asm;
	}
	public SchedulerManagerInterface getStm() {
		return stm;
	}
	public void setStm(SchedulerManagerInterface stm) {
		this.stm = stm;
	}
	public String getServerId() {
		return serverId;
	}
	public void setServerId(String serverId) {
		this.serverId = serverId;
	}
	public String getGroupName() {
		return groupName;
	}
	public void setGroupName(String groupName) {
		this.groupName = groupName;
	}
}
