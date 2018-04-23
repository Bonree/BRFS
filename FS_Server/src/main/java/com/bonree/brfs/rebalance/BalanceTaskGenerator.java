package com.bonree.brfs.rebalance;

import java.util.List;

import com.bonree.brfs.rebalance.task.BalanceTaskSummary;
import com.bonree.brfs.rebalance.task.ChangeSummary;

public interface BalanceTaskGenerator {

    /** 概述：生成虚拟SID迁移任务
     * @param virtualId
     * @param changeSummary
     * @return
     * @user <a href=mailto:weizheng@bonree.com>魏征</a>
     */
    BalanceTaskSummary genVirtualTask(String changeID, int storageIndex, String virtualId, String selectID, List<String> participators);

    /** 概述：生成普通的SID迁移任务
     * @param changeSummary
     * @return
     * @user <a href=mailto:weizheng@bonree.com>魏征</a>
     */
    BalanceTaskSummary genBalanceTask(ChangeSummary changeSummary);

}
