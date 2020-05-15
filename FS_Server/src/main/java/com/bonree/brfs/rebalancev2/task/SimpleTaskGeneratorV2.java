package com.bonree.brfs.rebalancev2.task;

import com.bonree.brfs.rebalance.DataRecover.RecoverType;
import com.bonree.brfs.rebalance.task.TaskStatus;
import com.bonree.brfs.rebalancev2.BalanceTaskGeneratorV2;
import java.util.List;
import java.util.Map;

/*******************************************************************************
 * 版权信息：博睿宏远科技发展有限公司
 * Copyright: Copyright (c) 2007博睿宏远科技发展有限公司,Inc.All Rights Reserved.
 *
 * @date 2018年4月18日 下午4:43:40
 * @Author: <a href=mailto:weizheng@bonree.com>魏征</a>
 * @Description: 通过change 生成 task
 ******************************************************************************/
public class SimpleTaskGeneratorV2 implements BalanceTaskGeneratorV2 {

    @Override
    public BalanceTaskSummaryV2 genVirtualTask(String changeID, int storageIndex, String partitionId, String virtualId,
                                               List<String> selectIDs, List<String> participators,
                                               Map<String, Integer> newSecondIds, long delayTime) {

        BalanceTaskSummaryV2 taskSummary = new BalanceTaskSummaryV2();
        // changeID
        taskSummary.setChangeID(changeID);
        // 参与者Server，提供数据
        taskSummary.setOutputServers(participators);
        // 源ServerId
        taskSummary.setServerId(virtualId);
        // 因为是构建虚拟SID恢复，则inputServer只需要有一个Server
        taskSummary.setInputServers(selectIDs);
        // 设置任务状态
        taskSummary.setTaskStatus(TaskStatus.INIT);
        // 设置任务类型
        taskSummary.setTaskType(RecoverType.VIRTUAL);
        // 设置SN的index
        taskSummary.setStorageIndex(storageIndex);

        // 设置任务延迟触发时间
        taskSummary.setDelayTime(delayTime); // 1分钟后开始迁移

        taskSummary.setPartitionId(partitionId);
        taskSummary.setNewSecondIds(newSecondIds);

        return taskSummary;
    }

    @Override
    public BalanceTaskSummaryV2 genBalanceTask(String changeID, int storageIndex, String partitionId, String secondServerID,
                                               List<String> selectIDs, List<String> participators,
                                               Map<String, Integer> newSecondIds, long delayTime) {
        BalanceTaskSummaryV2 taskSummary = new BalanceTaskSummaryV2();

        taskSummary.setChangeID(changeID);
        taskSummary.setServerId(secondServerID);
        taskSummary.setInputServers(selectIDs);
        taskSummary.setOutputServers(participators);
        taskSummary.setAliveServer(selectIDs);
        taskSummary.setStorageIndex(storageIndex);
        taskSummary.setTaskType(RecoverType.NORMAL);
        taskSummary.setTaskStatus(TaskStatus.INIT);
        taskSummary.setDelayTime(delayTime);
        taskSummary.setPartitionId(partitionId);
        taskSummary.setNewSecondIds(newSecondIds);

        return taskSummary;
    }

}
