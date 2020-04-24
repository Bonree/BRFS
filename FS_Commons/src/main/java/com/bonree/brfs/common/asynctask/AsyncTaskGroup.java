package com.bonree.brfs.common.asynctask;

import java.util.ArrayList;
import java.util.List;

/**
 * 任务组
 *
 * @param <V>
 *
 * @author chen
 */
public class AsyncTaskGroup<V> {
    private List<AsyncTask<V>> taskList = new ArrayList<AsyncTask<V>>();

    public int size() {
        return taskList.size();
    }

    public boolean isEmpty() {
        return taskList.isEmpty();
    }

    public void addTask(AsyncTask<V> task) {
        taskList.add(task);
    }

    public List<AsyncTask<V>> getTaskList() {
        return taskList;
    }
}
