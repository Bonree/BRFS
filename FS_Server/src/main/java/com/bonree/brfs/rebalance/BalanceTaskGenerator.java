package com.bonree.brfs.rebalance;

import com.bonree.brfs.rebalance.task.BalanceTaskSummary;
import java.util.List;
import java.util.Map;

public interface BalanceTaskGenerator {

    /**
     * 概述：生成虚拟SID迁移任务
     *
     * @param virtualId
     *
     * @return
     *
     * @user <a href=mailto:weizheng@bonree.com>魏征</a>
     */
    BalanceTaskSummary genVirtualTask(String changeID, int storageIndex, String partitionId, String virtualId,
                                      List<String> selectIDs, List<String> participators, Map<String, Integer> newSecondIds,
                                      String virtualTarget, long delayTime);

    /**
     * 概述：生成普通的SID迁移任务
     *
     * @return
     *
     * @user <a href=mailto:weizheng@bonree.com>魏征</a>
     */
    BalanceTaskSummary genBalanceTask(String changeID, int storageIndex, String partitionId, String secondServerID,
                                      List<String> selectIDs, List<String> participators, Map<String, Integer> newSecondIds,
                                      Map<String, String> secondFirstShip, long delayTime);

}
