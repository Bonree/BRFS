package com.bonree.brfs.common.asynctask;

/**
 * 任务组的执行结果callback接口
 *
 * @param <V>
 *
 * @author chen
 */
public interface AsyncTaskGroupCallback<V> {
    /**
     * 任务组中的所有任务完成后调用此方法
     *
     * @param results 任务的执行结果列表
     */
    void completed(AsyncTaskResult<V>[] results);
}
