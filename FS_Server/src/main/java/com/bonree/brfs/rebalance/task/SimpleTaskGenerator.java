package com.bonree.brfs.rebalance.task;

import java.util.List;

import org.apache.curator.shaded.com.google.common.collect.Lists;

import com.bonree.brfs.rebalance.BalanceTaskGenerator;
import com.bonree.brfs.rebalance.DataRecover.RecoverType;

/*******************************************************************************
 * 版权信息：博睿宏远科技发展有限公司
 * Copyright: Copyright (c) 2007博睿宏远科技发展有限公司,Inc.All Rights Reserved.
 * 
 * @date 2018年4月18日 下午4:43:40
 * @Author: <a href=mailto:weizheng@bonree.com>魏征</a>
 * @Description: 通过change 生成 task
 ******************************************************************************/
public class SimpleTaskGenerator implements BalanceTaskGenerator {

    @Override
    public BalanceTaskSummary genVirtualTask(String changeID, int storageIndex, String virtualId, String selectID, String participator, long delayTime) {

        BalanceTaskSummary taskSummary = new BalanceTaskSummary();
        // changeID
        taskSummary.setChangeID(changeID);
        // 参与者Server，提供数据
        taskSummary.setOutputServers(Lists.newArrayList(participator));
        // 源ServerId
        taskSummary.setServerId(virtualId);
        // 因为是构建虚拟SID恢复，则inputServer只需要有一个Server
        taskSummary.setInputServers(Lists.newArrayList(selectID));
        // 设置任务状态
        taskSummary.setTaskStatus(TaskStatus.INIT);
        // 设置任务类型
        taskSummary.setTaskType(RecoverType.VIRTUAL);
        // 设置SN的index
        taskSummary.setStorageIndex(storageIndex);

        // 设置任务延迟触发时间
        taskSummary.setDelayTime(delayTime); // 1分钟后开始迁移

        return taskSummary;
    }

    @Override
    public BalanceTaskSummary genBalanceTask(String changeID, int storageIndex, String secondServerID, List<String> selectIDs, List<String> participators, long delayTime) {
        BalanceTaskSummary taskSummary = new BalanceTaskSummary();

        taskSummary.setChangeID(changeID);

        taskSummary.setServerId(secondServerID);

        taskSummary.setInputServers(selectIDs);

        taskSummary.setOutputServers(participators);

        taskSummary.setAliveServer(selectIDs);

        taskSummary.setStorageIndex(storageIndex);

        taskSummary.setTaskType(RecoverType.NORMAL);

        taskSummary.setTaskStatus(TaskStatus.INIT);

        taskSummary.setDelayTime(delayTime);
        
        return taskSummary;
    }

}
