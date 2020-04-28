package com.bonree.brfs.schedulers.jobs.biz;

import com.bonree.brfs.common.utils.JsonUtils;
import com.bonree.brfs.schedulers.task.model.AtomTaskModel;
import com.bonree.brfs.schedulers.task.model.BatchAtomModel;
import com.bonree.brfs.schedulers.task.model.TaskModel;
import com.bonree.brfs.schedulers.utils.JobDataMapConstract;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BatchTaskFactory {
    /**
     * 概述：
     *
     * @param task
     * @param batchSize
     *
     * @return
     *
     * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
     */
    public static Map<String, String> createBatch(TaskModel task, int batchSize) {
        Map<String, String> batchDatas = new HashMap<String, String>();
        if (task == null) {
            return batchDatas;
        }
        List<AtomTaskModel> atoms = convernTaskModel(task);
        if (atoms == null || atoms.isEmpty()) {
            return batchDatas;
        }
        int size = atoms.size();
        int batchCount = size % batchSize == 0 ? size / batchSize : size / batchSize + 1;
        BatchAtomModel batch = null;
        List<AtomTaskModel> tmp = null;
        //若不足一次则按一次来
        if (batchCount == 0) {
            batch = new BatchAtomModel();
            batch.addAll(atoms);
            batchDatas.put(JobDataMapConstract.CURRENT_INDEX, "1");
            batchDatas.put("1", JsonUtils.toJsonStringQuietly(batch));
            return batchDatas;
        }
        batchDatas.put(JobDataMapConstract.CURRENT_INDEX, batchCount + "");
        int index = 0;
        for (int i = 1; i <= batchCount; i++) {
            batch = new BatchAtomModel();
            if (index >= size) {
                tmp = new ArrayList<>();
            } else if (index + batchSize > size) {
                tmp = atoms.subList(index, size);
            } else if (index + batchSize <= size) {
                tmp = atoms.subList(index, index + batchSize);
            }
            batch.addAll(tmp);
            batchDatas.put(i + "", JsonUtils.toJsonStringQuietly(batch));
            index = index + batchSize;
        }
        return batchDatas;
    }

    public static List<AtomTaskModel> convernTaskModel(TaskModel task) {
        List<AtomTaskModel> atoms = new ArrayList<AtomTaskModel>();
        List<AtomTaskModel> tasks = task.getAtomList();
        if (tasks == null || tasks.isEmpty()) {
            return atoms;
        }
        atoms.addAll(tasks);
        return atoms;
    }

    public static void main(String[] args) {
        String content =
            "{\"taskType\":1,\"taskState\":1,\"createTime\""
                + ":\"2020-04-27 22:00:28.574\",\"atomList\":[{\"storageName\":\"lq\","
                + "\"dataStartTime\":\"2020-04-27 20:50:00.000\","
                + "\"dataStopTime\":\"2020-04-27 21:00:00.000\",\"files\":[],"
                + "\"taskOperation\":\"\",\"granule\":600000,\"patitionNum\":2}]}";
        TaskModel task = JsonUtils.toObjectQuietly(content, TaskModel.class);
        Map<String, String> map = createBatch(task, 3);
        System.out.println(map);

    }
}
