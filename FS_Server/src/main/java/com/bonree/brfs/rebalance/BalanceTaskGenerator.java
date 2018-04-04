package com.bonree.brfs.rebalance;

import com.bonree.brfs.rebalance.task.BalanceTaskSummary;
import com.bonree.brfs.rebalance.task.ChangeSummary;

public interface BalanceTaskGenerator {

    /** 概述：生成虚拟SID迁移任务
     * @param virtualId
     * @param changeSummary
     * @return
     * @user <a href=mailto:weizheng@bonree.com>魏征</a>
     */
    BalanceTaskSummary genVirtualTask(String virtualId, ChangeSummary changeSummary);

    /** 概述：生成普通的SID迁移任务
     * @param changeSummary
     * @return
     * @user <a href=mailto:weizheng@bonree.com>魏征</a>
     */
    BalanceTaskSummary genBalanceTask(ChangeSummary changeSummary);

}
