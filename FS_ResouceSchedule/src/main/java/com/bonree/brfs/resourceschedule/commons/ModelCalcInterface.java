package com.bonree.brfs.resourceschedule.commons;

import java.util.List;
/*****************************************************************************
 * 版权信息：北京博睿宏远数据科技股份有限公司
 * Copyright: Copyright (c) 2007北京博睿宏远数据科技股份有限公司,Inc.All Rights Reserved.
 * 
 * @date 2018年3月21日 下午4:36:56
 * @Author: <a href=mailto:zhucg@bonree.com>朱成岗</a>
 * @Description: model计算接口
 *****************************************************************************
 */
public interface ModelCalcInterface <T> {	
	/**
	 * 概述：计算model
	 * @param t1
	 * @return
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	T calc(T t1);
	/**
	 * 概述：将多个model汇总成一个
	 * @param t1
	 * @return
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	T sum(T t1);
}
