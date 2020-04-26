package com.bonree.brfs.rebalancev2;

import com.bonree.brfs.rebalancev2.task.BalanceTaskSummaryV2;
import java.util.List;

public interface BalanceTaskGeneratorV2 {

    /**
     * 概述：生成虚拟SID迁移任务
     *
     * @param virtualId
     * @param changeSummary
     *
     * @return
     *
     * @user <a href=mailto:weizheng@bonree.com>魏征</a>
     */
    BalanceTaskSummaryV2 genVirtualTask(String changeID, int storageIndex, String partitionId, String virtualId, String selectID,
                                        String participator, long delayTime);

    /**
     * 概述：生成普通的SID迁移任务
     *
     * @param changeSummary
     *
     * @return
     *
     * @user <a href=mailto:weizheng@bonree.com>魏征</a>
     */
    BalanceTaskSummaryV2 genBalanceTask(String changeID, int storageIndex, String partitionId, String secondServerID,
                                        List<String> selectIDs, List<String> participators, long delayTime);

}
