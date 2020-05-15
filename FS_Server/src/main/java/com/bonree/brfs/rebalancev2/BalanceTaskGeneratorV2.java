package com.bonree.brfs.rebalancev2;

import com.bonree.brfs.rebalancev2.task.BalanceTaskSummaryV2;
import java.util.List;
import java.util.Map;

public interface BalanceTaskGeneratorV2 {

    /**
     * 概述：生成虚拟SID迁移任务
     *
     * @param virtualId
     *
     * @return
     *
     * @user <a href=mailto:weizheng@bonree.com>魏征</a>
     */
    BalanceTaskSummaryV2 genVirtualTask(String changeID, int storageIndex, String partitionId, String virtualId,
                                        List<String> selectIDs, List<String> participators, Map<String, Integer> newSecondIds,
                                        long delayTime);

    /**
     * 概述：生成普通的SID迁移任务
     *
     * @return
     *
     * @user <a href=mailto:weizheng@bonree.com>魏征</a>
     */
    BalanceTaskSummaryV2 genBalanceTask(String changeID, int storageIndex, String partitionId, String secondServerID,
                                        List<String> selectIDs, List<String> participators, Map<String, Integer> newSecondIds,
                                        long delayTime);

}
