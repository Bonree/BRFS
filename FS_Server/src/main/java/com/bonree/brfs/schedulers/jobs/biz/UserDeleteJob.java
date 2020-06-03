package com.bonree.brfs.schedulers.jobs.biz;

import com.bonree.brfs.common.files.impl.BRFSTimeFilter;
import com.bonree.brfs.common.utils.BRFSFileUtil;
import com.bonree.brfs.common.utils.BRFSPath;
import com.bonree.brfs.common.utils.BrStringUtils;
import com.bonree.brfs.common.utils.FileUtils;
import com.bonree.brfs.common.utils.JsonUtils;
import com.bonree.brfs.common.utils.TimeUtils;
import com.bonree.brfs.duplication.storageregion.StorageRegion;
import com.bonree.brfs.duplication.storageregion.StorageRegionManager;
import com.bonree.brfs.identification.impl.DiskDaemon;
import com.bonree.brfs.partition.model.LocalPartitionInfo;
import com.bonree.brfs.schedulers.ManagerContralFactory;
import com.bonree.brfs.schedulers.task.model.AtomTaskModel;
import com.bonree.brfs.schedulers.task.model.AtomTaskResultModel;
import com.bonree.brfs.schedulers.task.model.BatchAtomModel;
import com.bonree.brfs.schedulers.task.model.TaskResultModel;
import com.bonree.brfs.schedulers.task.operation.impl.QuartzOperationStateWithZKTask;
import com.bonree.brfs.schedulers.utils.JobDataMapConstract;
import com.bonree.brfs.schedulers.utils.TaskStateLifeContral;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import org.quartz.UnableToInterruptJobException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/******************************************************************************
 * 版权信息：北京博睿宏远数据科技股份有限公司
 * Copyright: Copyright (c) 2007北京博睿宏远数据科技股份有限公司,Inc.All Rights Reserved.
 * @date 2018年5月3日 下午4:29:44
 * @Author: <a href=mailto:zhucg@bonree.com>朱成岗</a>
 * @Description:系统删除任务
 *****************************************************************************
 */
public class UserDeleteJob extends QuartzOperationStateWithZKTask {
    public static final String DELETE_SN_ALL = "0";
    public static final String DELETE_PART = "1";
    private static final Logger LOG = LoggerFactory.getLogger(UserDeleteJob.class);

    @Override
    public void interrupt() throws UnableToInterruptJobException {
    }

    @Override
    public void operation(JobExecutionContext context) throws Exception {
        JobDataMap data = context.getJobDetail().getJobDataMap();
        String currentIndex = data.getString(JobDataMapConstract.CURRENT_INDEX);
        String content = data.getString(currentIndex);
        // 获取当前执行的任务类型
        if (BrStringUtils.isEmpty(content)) {
            LOG.debug("batch data is empty !!!");
            return;
        }
        BatchAtomModel batch = JsonUtils.toObject(content, BatchAtomModel.class);
        if (batch == null) {
            LOG.debug("batch data is empty !!!");
            return;
        }

        List<AtomTaskModel> atoms = batch.getAtoms();
        if (atoms == null || atoms.isEmpty()) {
            LOG.debug("atom task is empty !!!");
            return;
        }
        String snName;
        TaskResultModel result = new TaskResultModel();
        AtomTaskResultModel usrResult;
        List<String> sns = new ArrayList<String>();
        String operation;
        DiskDaemon daemon = ManagerContralFactory.getInstance().getDaemon();
        for (AtomTaskModel atom : atoms) {
            snName = atom.getStorageName();
            if ("1".equals(currentIndex)) {
                operation = atom.getTaskOperation();
                LOG.info("task operation {} ",
                         DELETE_SN_ALL.equals(operation) ? "Delete_Storage_Region" : "Delete_Part_Of_Storage_Region_Data");
                if (DELETE_SN_ALL.equals(operation)) {
                    sns.add(snName);
                }
            }
            usrResult = deleteFiles(atom, daemon);
            if (usrResult == null) {
                continue;
            }
            if (!usrResult.isSuccess()) {
                result.setSuccess(false);
            }
            result.add(usrResult);

        }
        StorageRegionManager snManager = ManagerContralFactory.getInstance().getSnm();
        if ("1".equals(currentIndex)) {
            for (String sn : sns) {
                StorageRegion region = snManager.findStorageRegionByName(sn);
                if (region != null) {
                    LOG.warn("skip delete {} directory ! because a new one is alive", sn);
                    continue;
                }
                for (LocalPartitionInfo local : daemon.getPartitions()) {
                    if (FileUtils.deleteDir(local.getDataDir() + "/" + sn, true)) {
                        LOG.info("deltete {} successfull", sn);
                    } else {
                        result.setSuccess(false);
                        LOG.warn("delete sn dir error {}", local.getDataDir() + "/" + sn);
                    }
                }
            }
        }
        //更新任务状态
        TaskStateLifeContral.updateMapTaskMessage(context, result);
    }

    /**
     * 概述：封装执行结果
     *
     * @param atom
     *
     * @return
     *
     * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
     */
    public AtomTaskResultModel deleteFiles(AtomTaskModel atom, DiskDaemon daemon) {
        if (atom == null) {
            return null;
        }
        String snName = atom.getStorageName();
        long startTime = TimeUtils.getMiles(atom.getDataStartTime(), TimeUtils.TIME_MILES_FORMATE);
        long endTime = TimeUtils.getMiles(atom.getDataStopTime(), TimeUtils.TIME_MILES_FORMATE);
        AtomTaskResultModel atomR = new AtomTaskResultModel();
        atomR.setDataStartTime(TimeUtils.formatTimeStamp(startTime, TimeUtils.TIME_MILES_FORMATE));
        atomR.setDataStopTime(TimeUtils.formatTimeStamp(endTime, TimeUtils.TIME_MILES_FORMATE));
        atomR.setPartNum(atom.getPatitionNum());
        atomR.setSn(snName);
        for (LocalPartitionInfo path : daemon.getPartitions()) {

            Map<String, String> snMap = new HashMap<>();
            snMap.put(BRFSPath.STORAGEREGION, snName);
            List<BRFSPath> deleteDirs =
                BRFSFileUtil.scanBRFSFiles(path.getDataDir(), snMap, snMap.size(), new BRFSTimeFilter(startTime, endTime));
            LOG.debug("collection {}_{} dirs {}", atom.getDataStartTime(), atom.getDataStopTime(), deleteDirs);
            if (deleteDirs == null || deleteDirs.isEmpty()) {
                atomR.setOperationFileCount(0);
                continue;
            }
            boolean isSuccess = true;
            for (BRFSPath deletePath : deleteDirs) {
                isSuccess =
                    isSuccess && FileUtils.deleteDir(path.getDataDir() + FileUtils.FILE_SEPARATOR + deletePath.toString(), true);
                if (!isSuccess) {
                    LOG.info("delete [{}], status [{}]", deletePath, isSuccess);
                }
            }
            atomR.setOperationFileCount(atomR.getOperationFileCount() + deleteDirs.size());
            atomR.setSuccess(atomR.isSuccess() && isSuccess);
        }
        return atomR;
    }
}
