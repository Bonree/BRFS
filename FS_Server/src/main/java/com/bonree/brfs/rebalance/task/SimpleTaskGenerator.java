package com.bonree.brfs.rebalance.task;

import org.apache.curator.shaded.com.google.common.collect.Lists;

import com.bonree.brfs.rebalance.BalanceTaskGenerator;

public class SimpleTaskGenerator implements BalanceTaskGenerator {

    @Override
    public BalanceTaskSummary genVirtualTask(String virtualId, ChangeSummary changeSummary) {
        BalanceTaskSummary taskSummary = new BalanceTaskSummary();
        // 参与者Server，提供数据
        taskSummary.setOutputServers(changeSummary.getCurrentServers());
        // 源ServerId
        taskSummary.setServerId(virtualId);
        // 因为是构建虚拟SID恢复，则inputServer只需要有一个Server
        taskSummary.setInputServers(Lists.asList(changeSummary.getChangeServer(), null));
        // 设置任务状态
        taskSummary.setTaskStatus(TaskStatus.INIT);
        // 设置任务类型
        taskSummary.setTaskType(1);
        // 设置SN的index
        taskSummary.setStorageIndex(changeSummary.getStorageIndex());
        // 设置任务延迟触发时间
        taskSummary.setRuntime(60 * 1); // 1分钟后开始迁移
        return taskSummary;
    }

    @Override
    public BalanceTaskSummary genBalanceTask(ChangeSummary changeSummary) { // TODO 此处需要挑选机器
        BalanceTaskSummary taskSummary = new BalanceTaskSummary();
        taskSummary.setServerId(changeSummary.getChangeServer());
        taskSummary.setStorageIndex(changeSummary.getStorageIndex());
        taskSummary.setOutputServers(changeSummary.getCurrentServers());
        taskSummary.setInputServers(changeSummary.getCurrentServers());
        taskSummary.setTaskStatus(TaskStatus.INIT);
        taskSummary.setTaskType(2);
        taskSummary.setRuntime(60 * 30);
        return taskSummary;
    }

}
