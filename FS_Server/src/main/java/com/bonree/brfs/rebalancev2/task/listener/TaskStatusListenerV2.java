package com.bonree.brfs.rebalancev2.task.listener;

import com.bonree.brfs.rebalancev2.task.TaskDispatcherV2;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.TreeCacheEvent;
import org.apache.curator.framework.recipes.cache.TreeCacheListener;

/*******************************************************************************
 * 版权信息：博睿宏远科技发展有限公司
 * Copyright: Copyright (c) 2007博睿宏远科技发展有限公司,Inc.All Rights Reserved.
 *
 * @date 2018年4月19日 下午3:48:05
 * @Author: <a href=mailto:weizheng@bonree.com>魏征</a>
 * @Description: 监听任务是否完成，以便进行通知
 ******************************************************************************/
public class TaskStatusListenerV2 implements TreeCacheListener {

    TaskDispatcherV2 dispatch;

    public TaskStatusListenerV2(TaskDispatcherV2 dispatch) {
        this.dispatch = dispatch;
    }

    @Override
    public void childEvent(CuratorFramework client, TreeCacheEvent event) throws Exception {
        dispatch.syncTaskTerminal(client, event);
    }
}
