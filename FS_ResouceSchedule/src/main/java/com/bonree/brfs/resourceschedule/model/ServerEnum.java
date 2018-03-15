package com.bonree.brfs.resourceschedule.model;
/*****************************************************************************
 * 版权信息：北京博睿宏远数据科技股份有限公司
 * Copyright: Copyright (c) 2007北京博睿宏远数据科技股份有限公司,Inc.All Rights Reserved.
 * 
 * @date 2018年3月9日 下午5:11:09
 * @Author: <a href=mailto:zhucg@bonree.com>朱成岗</a>
 * @Description: 
 *****************************************************************************
 */
public class ServerEnum {

	public enum NET_ENUM{
		NET_DEVICE_NAME,
		IP_ADDRESS,
		MAC_ADDRESS,
		MAX_T_SPEED,
		MAX_R_SPEED,
		T_DATA_SIZE,
		R_DATA_SIZE
	}
	
	public enum PATITION_ENUM{
		MOUNT_POINT,
		PATITION_FORMAT,
		DISK_TYPE,
		PATITION_SIZE,
		MAX_WRITE_SPEED,
		MAX_READ_SPEED,
		USED_SIZE,
		REMAIN_SIZE,
		WIRTE_DATA_SIZE,
		READ_DATA_SIZE
	}
	public enum CPU_ENUM{
		CPU_CORE_COUNT,
		CPU_RATE,
		CPU_REMAIN_RATE
	}
	public enum MEMORY_ENUM{
		MEMORY_SIZE,
		MEMORY_RATE,
		MEMORY_REMAIN_RATE
	}
	public enum SERVER_COMMON_ENUM{
		SERVER_ID,
		SERVER_BASE_INFO,
		SERVER_STAT_INFO,
		NET_BASE_INFO,
		NET_STAT_INFO,
		PATITION_BASE_INFO,
		PATITION_STAT_INFO,
		CPU_STAT_INFO,
		MEMORY_STAT_INFO
	}
}
