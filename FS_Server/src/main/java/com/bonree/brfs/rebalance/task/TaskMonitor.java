package com.bonree.brfs.rebalance.task;

import java.util.List;

import org.apache.curator.utils.ZKPaths;

import com.bonree.brfs.common.utils.JsonUtils;
import com.bonree.brfs.common.zookeeper.curator.CuratorClient;

/*******************************************************************************
 * 版权信息：博睿宏远科技发展有限公司
 * Copyright: Copyright (c) 2007博睿宏远科技发展有限公司,Inc.All Rights Reserved.
 * 
 * @date 2018年3月30日 下午4:33:34
 * @Author: <a href=mailto:weizheng@bonree.com>魏征</a>
 * @Description: 此类为监视副本恢复时的任务进度和状态
 ******************************************************************************/
public class TaskMonitor {

    /** 概述：获取某个任务的进度
     * @param client
     * @param taskPath
     * @return
     * @user <a href=mailto:weizheng@bonree.com>魏征</a>
     */
    public double getTaskProgress(CuratorClient client, String taskPath) {
        double process = 0.0;
        int curent = 0;
        int total = 0;
        if (!client.checkExists(taskPath)) {
            return process;
        }
        List<String> joiners = client.getChildren(taskPath);
        if (joiners != null && !joiners.isEmpty()) {
            for (String joiner : joiners) {
                String joinerPath = ZKPaths.makePath(taskPath, joiner);
                byte[] data = client.getData(joinerPath);
                TaskDetail detail = JsonUtils.toObjectQuietly(data, TaskDetail.class);
                curent += detail.getCurentCount();
                total += detail.getTotalDirectories();
            }
            if(total == 0) {
                return process;
            }
            process = curent / (double) total;
        }
        return process;
    }
}
