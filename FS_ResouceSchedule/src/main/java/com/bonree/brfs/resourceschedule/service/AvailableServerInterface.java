package com.bonree.brfs.resourceschedule.service;

import java.util.List;

import com.bonree.brfs.resourceschedule.model.ResourceModel;

/*****************************************************************************
 * 版权信息：北京博睿宏远数据科技股份有限公司
 * Copyright: Copyright (c) 2007北京博睿宏远数据科技股份有限公司,Inc.All Rights Reserved.
 * 
 * @date 2018年3月16日 上午10:49:46
 * @Author: <a href=mailto:zhucg@bonree.com>朱成岗</a>
 * @Description:可用server接口 
 *****************************************************************************
 */
public interface AvailableServerInterface {
	/**
	 * 概述：获取可用server
	 * @param scene 场景枚举
	 * @return
	 * @throws Exception
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	public String selectAvailableServer(int scene) throws Exception;
	/**
	 * 概述：获取可用server
	 * @param scene 场景枚举
	 * @param exceptionServerList 异常server集合
	 * @return
	 * @throws Exception
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	public String selectAvailableServer(int scene,List<String> exceptionServerList) throws Exception;
	/**
	 * 概述：获取可用server集合
	 * @param scene 场景枚举
	 * @return
	 * @throws Exception
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	public List<String> selectAvailableServers(int scene) throws Exception;
	/**
	 * 概述：获取可用server集合
	 * @param scene 场景枚举
	 * @param exceptionServerList 异常server集合
	 * @return
	 * @throws Exception
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	public List<String> selectAvailableServers(int scene, List<String> exceptionServerList) throws Exception;
}
