package com.bonree.brfs.common.zookeeper.curator.locking;

import com.bonree.brfs.common.zookeeper.curator.CuratorZookeeperClient;

/*******************************************************************************
 * 版权信息：博睿宏远科技发展有限公司
 * Copyright: Copyright (c) 2007博睿宏远科技发展有限公司,Inc.All Rights Reserved.
 * 
 * @date 2018年3月12日 下午6:39:47
 * @Author: <a href=mailto:weizheng@bonree.com>魏征</a>
 * @Description: 
 ******************************************************************************/
public interface Executor {
    
    void execute(CuratorZookeeperClient client);
}
