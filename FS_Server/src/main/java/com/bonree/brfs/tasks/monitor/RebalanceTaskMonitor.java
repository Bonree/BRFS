package com.bonree.brfs.tasks.monitor;

/**
 * 副本平衡任务监控接口
 */
public interface RebalanceTaskMonitor {
    /**
     * 判断副本平衡任务是否执行，执行返回true，不执行返回false
     *
     * @return
     */
    boolean isExecute();
}
