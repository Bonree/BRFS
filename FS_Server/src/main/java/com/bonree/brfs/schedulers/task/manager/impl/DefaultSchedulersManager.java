package com.bonree.brfs.schedulers.task.manager.impl;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bonree.brfs.common.utils.BrStringUtils;
import com.bonree.brfs.schedulers.exception.ParamsErrorException;
import com.bonree.brfs.schedulers.task.manager.BaseSchedulerInterface;
import com.bonree.brfs.schedulers.task.manager.SchedulerManagerInterface;
import com.bonree.brfs.schedulers.task.meta.SumbitTaskInterface;
/******************************************************************************
 * 版权信息：北京博睿宏远数据科技股份有限公司
 * Copyright: Copyright (c) 2007北京博睿宏远数据科技股份有限公司,Inc.All Rights Reserved.
 * 
 * @date 2018年3月28日 下午4:19:33
 * @Author: <a href=mailto:zhucg@bonree.com>朱成岗</a>
 * @Description: 单例模式的调度接口
 *****************************************************************************
 */
public class DefaultSchedulersManager implements SchedulerManagerInterface<String, BaseSchedulerInterface, SumbitTaskInterface>{
	Map<String,BaseSchedulerInterface> taskPoolMap = new ConcurrentHashMap<String,BaseSchedulerInterface>();
	private static final Logger LOG = LoggerFactory.getLogger("TaskManagerServer");
	private static class SingletonInstance {
		public static DefaultSchedulersManager instance = new DefaultSchedulersManager();
	}
	private DefaultSchedulersManager() {
	}
	public static DefaultSchedulersManager getInstance() {
		return SingletonInstance.instance;
	}
	@Override
	public boolean addTask(String taskpoolkey, SumbitTaskInterface task) throws ParamsErrorException {
		checkParams(taskpoolkey, task);
		BaseSchedulerInterface pool = taskPoolMap.get(taskpoolkey);
		if(pool == null){
			return false;
		}
		try {
			return pool.addTask(task);
		}
		catch (Exception e) {
			e.printStackTrace();
			return false;
		}
	}

	@Override
	public boolean pauseTask(String taskpoolkey, SumbitTaskInterface task)  throws ParamsErrorException {
		checkParams(taskpoolkey, task);
		BaseSchedulerInterface pool = taskPoolMap.get(taskpoolkey);
		if(pool == null){
			return false;
		}
		try {
			pool.pauseTask(task);
		}
		catch (Exception e) {
			e.printStackTrace();
			return false;
		}
		return true;
	}

	@Override
	public boolean resumeTask(String taskpoolkey, SumbitTaskInterface task)  throws ParamsErrorException {
		checkParams(taskpoolkey, task);
		BaseSchedulerInterface pool = taskPoolMap.get(taskpoolkey);
		if(pool == null){
			return false;
		}
		try {
			pool.resumeTask(task);
		}
		catch (Exception e) {
			e.printStackTrace();
			return false;
		}
		return true;
	}

	@Override
	public boolean deleteTask(String taskpoolkey, SumbitTaskInterface task) throws ParamsErrorException  {
		checkParams(taskpoolkey, task);
		BaseSchedulerInterface pool = taskPoolMap.get(taskpoolkey);
		if (pool == null) {
			return false;
		}
		try {
			pool.deleteTask(task);
		}
		catch (Exception e) {
			e.printStackTrace();
			return false;
		}
		return true;
	}

	@Override
	public boolean createTaskPool(String taskpoolKey, Properties prop) throws ParamsErrorException {
		if(BrStringUtils.isEmpty(taskpoolKey)){
			throw new ParamsErrorException("task pool key is empty !!!");
		}
		if (taskPoolMap.containsKey(taskpoolKey)) {
			throw new ParamsErrorException(taskpoolKey + " task pool key is exists !!!");
		}
		BaseSchedulerInterface pool = new DefaultBaseSchedulers();
		String name = prop.getProperty("org.quartz.scheduler.instanceName");
		if(BrStringUtils.isEmpty(name)){
			prop.setProperty("org.quartz.scheduler.instanceName", taskpoolKey);
		}
		try {
			pool.initProperties(prop);
			this.taskPoolMap.put(taskpoolKey, pool);
		}
		catch (Exception e) {
			LOG.error("Exception : {}",e);
			return false;
		}
		return true;
	}

	@Override
	public boolean startTaskPool(String taskpoolKey) throws ParamsErrorException  {
		if(BrStringUtils.isEmpty(taskpoolKey)){
			throw new ParamsErrorException("task pool key is empty !!!");
		}
		if (!taskPoolMap.containsKey(taskpoolKey)) {
			throw new ParamsErrorException("task pool key is not exists !!!");
		}
		BaseSchedulerInterface pool = taskPoolMap.get(taskpoolKey);
		if (pool == null) {
			return false;
		}
		try {
			if(pool.isStart()){
				return false;
			}
			pool.start();
		}
		catch (Exception e) {
			e.printStackTrace();
			return false;
		}
		return true;
	}
	@Override
	public boolean reStartTaskPool(String taskpoolKey) throws ParamsErrorException  {
		if(BrStringUtils.isEmpty(taskpoolKey)){
			throw new ParamsErrorException("task pool key is empty !!!");
		}
		if (!taskPoolMap.containsKey(taskpoolKey)) {
			throw new ParamsErrorException("task pool key is not exists !!!");
		}
		BaseSchedulerInterface pool = taskPoolMap.get(taskpoolKey);
		if (pool == null) {
			return false;
		}
		try {
			if(!pool.isDestory()){
				return false;
			}
			pool.reStart();
		}
		catch (Exception e) {
			e.printStackTrace();
			return false;
		}
		return true;
	}

	@Override
	public boolean pauseTaskPool(String taskpoolKey) throws ParamsErrorException {
		if(BrStringUtils.isEmpty(taskpoolKey)){
			throw new ParamsErrorException("task pool key is empty !!!");
		}
		if (!taskPoolMap.containsKey(taskpoolKey)) {
			throw new ParamsErrorException("task pool key is not exists !!!");
		}
		BaseSchedulerInterface pool = taskPoolMap.get(taskpoolKey);
		if (pool == null) {
			return false;
		}
		try {
			pool.PausePool();
		}
		catch (Exception e) {
			e.printStackTrace();
			return false;
		}
		return true;
	}

	@Override
	public boolean resumeTaskPool(String taskpoolKey)throws ParamsErrorException{
		if(BrStringUtils.isEmpty(taskpoolKey)){
			throw new ParamsErrorException("task pool key is empty !!!");
		}
		if (!taskPoolMap.containsKey(taskpoolKey)) {
			throw new ParamsErrorException("task pool key is not exists !!!");
		}
		BaseSchedulerInterface pool = taskPoolMap.get(taskpoolKey);
		if (pool == null) {
			return false;
		}
		try {
			pool.resumePool();
		}
		catch (Exception e) {
			e.printStackTrace();
			return false;
		}
		return true;
	}
	
	private void checkParams(String taskpoolKey, SumbitTaskInterface task) throws ParamsErrorException{
		if(BrStringUtils.isEmpty(taskpoolKey)){
			throw new ParamsErrorException("task pool key is empty !!!");
		}
		if(task == null){
			throw new ParamsErrorException("task is empty !!!");
		}
		if(!taskPoolMap.containsKey(taskpoolKey)){
			throw new ParamsErrorException("task pool key : "+ taskpoolKey+" is not exists !!!");
		}
	}

	@Override
	public Collection<String> getAllPoolKey() {
		return this.taskPoolMap.keySet();
	}

	@Override
	public Collection<String> getStartedPoolKeys() {
		return getState(0);
	}

	@Override
	public Collection<String> getClosedPoolKeys() {
		return getState(2);
	}

	@Override
	public Collection<String> getPausePoolKeys() {
		return getState(1);
	}
	private Collection<String> getState(int type){
		List<String> set = new ArrayList<String>();
		if(this.taskPoolMap.isEmpty()){
			return set;
		}
		String key = null;
		BaseSchedulerInterface tmp = null;
		for(Map.Entry<String, BaseSchedulerInterface> entry : this.taskPoolMap.entrySet()){
			key = entry.getKey();
			tmp = entry.getValue();
			if(type == 0 && tmp.isStart()){
				set.add(key);
			}else if(type == 1 && tmp.isPaused()){
				set.add(key);
			}else if(type == 2 && tmp.isDestory()){
				set.add(key);
			}
		}
		return set;
	}
	@Override
	public boolean isStarted(String taskpoolKey) throws ParamsErrorException {
		return getState(taskpoolKey, 0);
	}

	@Override
	public boolean isClosed(String taskpoolKey) throws ParamsErrorException {
		return getState(taskpoolKey, 2);
	}

	@Override
	public boolean isPaused(String taskpoolKey) throws ParamsErrorException {
		return getState(taskpoolKey, 1);
	}
	private boolean getState(String taskpoolKey, int type) throws ParamsErrorException {
		if(BrStringUtils.isEmpty(taskpoolKey)){
			throw new ParamsErrorException("task pool key is empty !!!");
		}
		if(!taskPoolMap.containsKey(taskpoolKey)){
			throw new ParamsErrorException("task pool key : "+ taskpoolKey+" is not exists !!!");
		}
		BaseSchedulerInterface pool = taskPoolMap.get(taskpoolKey);
		if (pool == null) {
			throw new ParamsErrorException("task pool key : "+ taskpoolKey+" is not exists !!!");
		}
		if(type == 0){
			return pool.isStart();
		}else if(type == 1){
			return pool.isPaused();
		}else if(type == 2){
			return pool.isDestory();
		}
		return false;
		
	}

	@Override
	public int getTaskStat(String taskpoolKey, SumbitTaskInterface task) throws ParamsErrorException {
		checkParams(taskpoolKey, task);
		BaseSchedulerInterface pool = taskPoolMap.get(taskpoolKey);
		if (pool == null) {
			return -2;
		}
		return pool.getTaskStat(task);
	}
	@Override
	public boolean destoryTaskPool(String taskpoolKey, boolean isWaitTaskCompleted) throws ParamsErrorException {
		if(BrStringUtils.isEmpty(taskpoolKey)){
			throw new ParamsErrorException("task pool key is empty !!!");
		}
		if (!taskPoolMap.containsKey(taskpoolKey)) {
			throw new ParamsErrorException("task pool key is not exists !!!");
		}
		BaseSchedulerInterface pool = taskPoolMap.get(taskpoolKey);
		if (pool == null) {
			return true;
		}
		try {
			pool.close(isWaitTaskCompleted);
			this.taskPoolMap.remove(taskpoolKey);
		}
		catch (Exception e) {
			e.printStackTrace();
			return false;
		}
		return true;
	}
	@Override
	public boolean closeTaskPool(String taskpoolKey, boolean isWaitTaskCompleted) throws ParamsErrorException {
		if(BrStringUtils.isEmpty(taskpoolKey)){
			throw new ParamsErrorException("task pool key is empty !!!");
		}
		if (!taskPoolMap.containsKey(taskpoolKey)) {
			throw new ParamsErrorException("task pool key is not exists !!!");
		}
		BaseSchedulerInterface pool = taskPoolMap.get(taskpoolKey);
		if (pool == null) {
			return true;
		}
		try {
			pool.close(isWaitTaskCompleted);
		}
		catch (Exception e) {
			e.printStackTrace();
			return false;
		}
		return true;
	}
	@Override
	public boolean pauseAllTask(String taskpoolKey) throws ParamsErrorException {
		if(BrStringUtils.isEmpty(taskpoolKey)){
			throw new ParamsErrorException("task pool key is empty !!!");
		}
		if (!taskPoolMap.containsKey(taskpoolKey)) {
			throw new ParamsErrorException("task pool key is not exists !!!");
		}
		BaseSchedulerInterface pool = taskPoolMap.get(taskpoolKey);
		if (pool == null) {
			return false;
		}
		try {
			pool.pauseAllTask();
		}
		catch (Exception e) {
			e.printStackTrace();
			return false;
		}
		return true;
	}
	@Override
	public boolean resumeAllTask(String taskpoolKey) throws ParamsErrorException {
		if(BrStringUtils.isEmpty(taskpoolKey)){
			throw new ParamsErrorException("task pool key is empty !!!");
		}
		if (!taskPoolMap.containsKey(taskpoolKey)) {
			throw new ParamsErrorException("task pool key is not exists !!!");
		}
		BaseSchedulerInterface pool = taskPoolMap.get(taskpoolKey);
		if (pool == null) {
			return false;
		}
		try {
			pool.resumeAllTask();
		}
		catch (Exception e) {
			e.printStackTrace();
			return false;
		}
		return true;
	}
	@Override
	public int getSumbitedTaskCount(String taskpoolKey) throws ParamsErrorException {
		if(BrStringUtils.isEmpty(taskpoolKey)){
			LOG.warn("taskpoolKey is empty");
			return 0;
		}
		if (!taskPoolMap.containsKey(taskpoolKey)) {
			LOG.warn("{} is not exists", taskpoolKey);
			return 0;
		}
		BaseSchedulerInterface pool = taskPoolMap.get(taskpoolKey);
		if (pool == null) {
			LOG.warn("{}' thread pool is null");
			return 0;
		}
		try {
			return pool.getSumbitTaskCount();
		}
		catch (Exception e) {
			e.printStackTrace();
			return 0;
		}
	}
	@Override
	public int getTaskPoolSize(String taskpoolKey) throws ParamsErrorException {
		if(BrStringUtils.isEmpty(taskpoolKey)){
			LOG.warn("taskpoolKey is empty");
			return 0;
		}
		if (!taskPoolMap.containsKey(taskpoolKey)) {
			LOG.warn("{} is not exists", taskpoolKey);
			return 0;
		}
		BaseSchedulerInterface pool = taskPoolMap.get(taskpoolKey);
		if (pool == null) {
			LOG.warn("{}' thread pool is null");
			return 0;
		}
		try {
			return pool.getPoolSize();
		}catch (Exception e) {
			e.printStackTrace();
			return 0;
		}
	}
}
