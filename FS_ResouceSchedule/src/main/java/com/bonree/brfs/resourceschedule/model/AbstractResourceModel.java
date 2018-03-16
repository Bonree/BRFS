package com.bonree.brfs.resourceschedule.model;

import com.alibaba.fastjson.JSONObject;
/*****************************************************************************
 * 版权信息：北京博睿宏远数据科技股份有限公司
 * Copyright: Copyright (c) 2007北京博睿宏远数据科技股份有限公司,Inc.All Rights Reserved.
 * 
 * @date 2018年3月16日 上午11:13:00
 * @Author: <a href=mailto:zhucg@bonree.com>朱成岗</a>
 * @Description: 资源状态抽象类
 *****************************************************************************
 */
public abstract class AbstractResourceModel {
	public abstract JSONObject toJSONObject();
}
