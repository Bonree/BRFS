package com.bonree.brfs.resourceschedule.commons;

import java.util.List;
import java.util.Map;
/******************************************************************************
 * 版权信息：北京博睿宏远数据科技股份有限公司
 * Copyright: Copyright (c) 2007北京博睿宏远数据科技股份有限公司,Inc.All Rights Reserved.
 * 
 * @date 2018年3月22日 下午6:18:40
 * @Author: <a href=mailto:zhucg@bonree.com>朱成岗</a>
 * @Description: map数据处理接口 
 *****************************************************************************
 */
public interface CommonMapCalcInterface<T1,T2> {
	/**
	 * 概述：map合并
	 * @param minuendMap
	 * @param subtrahendMap
	 * @return
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	Map<T1,T2> calcMapData(Map<T1,T2> minuendMap, Map<T1,T2> subtrahendMap);
	/**
	 * 概述：转换为list
	 * @param collectMap
	 * @param sourceMap
	 * @return
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	Map<T1,List<T2>> collectModels(Map<T1,List<T2>> collectMap, Map<T1,T2> sourceMap);
	/**
	 * 概述：汇总list数据
	 * @param collect
	 * @return
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	T2 sumList(List<T2> collect);
	/**
	 * 概述：汇总map
	 * @param collect
	 * @return
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	Map<T1,T2> sumMapData(Map<T1,List<T2>> collect);
}
