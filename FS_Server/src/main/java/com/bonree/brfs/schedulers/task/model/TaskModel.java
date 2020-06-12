package com.bonree.brfs.schedulers.task.model;

import com.bonree.brfs.common.task.TaskState;
import com.bonree.brfs.common.task.TaskType;
import com.bonree.brfs.common.utils.TimeUtils;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.util.ArrayList;
import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
public class TaskModel {
    /**
     * 任务类型taskType
     */
    private int taskType;
    /**
     * 任务状态，TaskStat
     */
    private int taskState;
    /**
     * 任务创建时间
     */
    private String createTime;
    /**
     * sn执行最小单元
     */
    private List<AtomTaskModel> atomList = new ArrayList<AtomTaskModel>();

    public static TaskModel getInitInstance(TaskType taskType) {
        TaskModel task = new TaskModel();
        task.setCreateTime(TimeUtils.formatTimeStamp(System.currentTimeMillis(), TimeUtils.TIME_MILES_FORMATE));
        task.setTaskState(TaskState.INIT.code());
        task.setTaskType(taskType.code());
        return task;
    }

    public int getTaskType() {
        return taskType;
    }

    public void setTaskType(int taskType) {
        this.taskType = taskType;
    }

    public int getTaskState() {
        return taskState;
    }

    public void setTaskState(int taskState) {
        this.taskState = taskState;
    }

    public List<AtomTaskModel> getAtomList() {
        return atomList;
    }

    public void setAtomList(List<AtomTaskModel> atomList) {
        this.atomList = atomList;
    }

    public void putAtom(List<AtomTaskModel> atoms) {
        this.atomList.addAll(atoms);
    }

    public void addAtom(AtomTaskModel atom) {
        this.atomList.add(atom);
    }

    public String getCreateTime() {
        return createTime;
    }

    public void setCreateTime(String createTime) {
        this.createTime = createTime;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("TaskModel{");
        sb.append("taskType=").append(taskType);
        sb.append(", taskState=").append(taskState);
        sb.append(", createTime='").append(createTime).append('\'');
        sb.append(", atomList=").append(atomList);
        sb.append('}');
        return sb.toString();
    }
}
