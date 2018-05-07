package com.bonree.brfs.schedulers;

import java.util.List;

import com.bonree.brfs.disknode.client.DiskNodeClient;

/******************************************************************************
 * 版权信息：北京博睿宏远数据科技股份有限公司
 * Copyright: Copyright (c) 2007北京博睿宏远数据科技股份有限公司,Inc.All Rights Reserved.
 * 
 * @date 2018年5月2日 上午11:02:51
 * @Author: <a href=mailto:zhucg@bonree.com>朱成岗</a>
 * @Description: 临时接口，用来对接俞朋的文件操作
 *****************************************************************************
 */
public interface SNOperation extends DiskNodeClient {
	List<String> getFiles(String path);
	boolean checkCRCFile(String path);
}
