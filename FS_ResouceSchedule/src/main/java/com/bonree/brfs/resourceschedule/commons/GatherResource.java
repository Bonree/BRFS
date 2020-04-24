package com.bonree.brfs.resourceschedule.commons;

import com.bonree.brfs.common.utils.BrStringUtils;
import com.bonree.brfs.common.utils.Pair;
import com.bonree.brfs.resourceschedule.model.BaseMetaServerModel;
import com.bonree.brfs.resourceschedule.model.ResourceModel;
import com.bonree.brfs.resourceschedule.model.StatServerModel;
import com.bonree.brfs.resourceschedule.model.StateMetaServerModel;
import com.bonree.brfs.resourceschedule.utils.CalcUtils;
import com.bonree.brfs.resourceschedule.utils.DiskUtils;
import com.bonree.brfs.resourceschedule.utils.SigarUtils;
import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import org.hyperic.sigar.SigarException;

/*****************************************************************************
 * 版权信息：北京博睿宏远数据科技股份有限公司
 * Copyright: Copyright (c) 2007北京博睿宏远数据科技股份有限公司,Inc.All Rights Reserved.
 *
 * @date 2018年3月22日 下午3:18:24
 * @Author: <a href=mailto:zhucg@bonree.com>朱成岗</a>
 * @Description: 统计信息
 *****************************************************************************
 */
public class GatherResource {

    /**
     * 概述：采集状态信息
     *
     * @return
     *
     * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
     */
    public static StateMetaServerModel gatherResource(String ip, Collection<String> mountPoints) {
        StateMetaServerModel obj = new StateMetaServerModel();
        try {
            if (BrStringUtils.isMathNumeric(ip)) {
                return null;
            }
            int cpuCore = SigarUtils.instance.gatherCpuCoreCount();
            obj.setCpuCoreCount(cpuCore);
            double cpuRate = SigarUtils.instance.gatherCpuRate();
            obj.setCpuRate(cpuRate);
            double memoryRate = SigarUtils.instance.gatherMemoryRate();
            obj.setMemoryRate(memoryRate);
            long memorySize = SigarUtils.instance.gatherMemSize();
            obj.setMemorySize(memorySize);
            Pair<Long, Long> netData = SigarUtils.instance.gatherNetStatInfos(ip);
            if (netData != null) {
                obj.setNetRByte(netData.getFirst());
                obj.setNetTByte(netData.getSecond());
            }
            Map<Integer, Map<String, Long>> partition = SigarUtils.instance.gatherPartitionInfo(mountPoints);
            if (partition.containsKey(0)) {
                obj.setPartitionTotalSizeMap(partition.get(0));
            }
            if (partition.containsKey(1)) {
                obj.setPartitionRemainSizeMap(partition.get(1));
            }
            if (partition.containsKey(2)) {
                obj.setPartitionReadByteMap(partition.get(2));
            }
            if (partition.containsKey(3)) {
                obj.setPartitionWriteByteMap(partition.get(3));
            }
        } catch (SigarException e) {
            e.printStackTrace();
        }
        return obj;
    }

    /**
     * 概述：基本信息
     *
     * @param dataDirs
     *
     * @return
     *
     * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
     */
    public static BaseMetaServerModel gatherBase(Collection<String> dataDirs) {
        BaseMetaServerModel obj = new BaseMetaServerModel();
        try {
            int cpuCore = SigarUtils.instance.gatherCpuCoreCount();
            obj.setCpuCoreCount(cpuCore);

            long memorySize = SigarUtils.instance.gatherMemSize();
            obj.setMemoryTotalSize(memorySize);
            Map<Integer, Map<String, Long>> partition = SigarUtils.instance.gatherPartitionInfo(dataDirs);
            if (partition.containsKey(0)) {
                long totalDiskSize = CalcUtils.collectDataMap(partition.get(0));
                obj.setDiskTotalSize(totalDiskSize);
            }
        } catch (SigarException e) {
            e.printStackTrace();
        }
        return obj;
    }

    /**
     * 概述：统计原始信息
     *
     * @param queue
     *
     * @return
     *
     * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
     */
    public static List<StatServerModel> calcState(Queue<StateMetaServerModel> queue, int pollSize) {
        List<StatServerModel> orderList = new ArrayList<StatServerModel>();
        if (queue == null) {
            return orderList;
        }
        int size = queue.size();
        if (size < 2) {
            return orderList;
        }
        StatServerModel obj;
        StateMetaServerModel pre = null;
        StateMetaServerModel current;
        if (size > pollSize) {
            int psize = size - pollSize;
            for (int i = 0; i < psize; i++) {
                if (i == 0 || pre == null) {
                    pre = queue.poll();
                    continue;
                }
                current = queue.poll();
                obj = current.converObject(pre);
                orderList.add(obj);
                pre = current;
            }

        }
        if (queue.isEmpty()) {
            return orderList;
        }
        Iterator<StateMetaServerModel> iterator = queue.iterator();
        StateMetaServerModel next;
        while (iterator.hasNext()) {
            next = iterator.next();
            if (pre == null) {
                pre = next;
                continue;
            }
            current = next;
            obj = current.converObject(pre);
            orderList.add(obj);
            pre = current;
        }
        return orderList;
    }

    /**
     * 概述：计算状态信息
     *
     * @param arrays
     * @param snList
     * @param inverTime
     *
     * @return
     *
     * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
     */
    public static StatServerModel calcStatServerModel(final List<StatServerModel> arrays, long inverTime) {
        if (arrays == null || arrays.isEmpty()) {
            return null;
        }
        StatServerModel obj = null;
        for (StatServerModel tmp : arrays) {
            if (obj == null) {
                obj = tmp;
                continue;
            }
            obj = tmp.sum(obj);
        }
        obj.calc(inverTime);
        return obj;
    }

    /**
     * 概述：汇总基础信息
     *
     * @param source
     *
     * @return
     *
     * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
     */
    public static BaseMetaServerModel collectBaseMetaServer(final Collection<BaseMetaServerModel> source) {
        BaseMetaServerModel obj = new BaseMetaServerModel();
        if (source == null) {
            return obj;
        }
        for (BaseMetaServerModel base : source) {
            obj = obj.sum(base);
        }
        return obj;
    }

    /***
     * 概述：计算resource
     * @param cluster
     * @param stat
     * @return
     * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
     */
    public static ResourceModel calcResourceValue(final BaseMetaServerModel cluster, final StatServerModel stat, String serverId,
                                                  String ip) {
        ResourceModel obj = new ResourceModel();
        Map<String, Double> cacheMap = null;
        long cacheNum = 0L;
        double cpuValue = (1 - stat.getCpuRate()) * stat.getCpuCoreCount() / cluster.getCpuCoreCount();
        double memoryValue = (1 - stat.getMemoryRate()) * stat.getMemorySize() / cluster.getMemoryTotalSize();
        double diskRemainRate = stat.getTotalDiskSize() == 0 ? 0.0 : (double) stat.getRemainDiskSize() / stat.getTotalDiskSize();
        obj.setServerId(serverId);
        obj.setHost(ip);
        obj.setCpuRate(stat.getCpuRate());
        obj.setMemoryRate(stat.getMemoryRate());
        obj.setDiskSize(stat.getTotalDiskSize());
        obj.setCpuValue(cpuValue);
        obj.setMemoryValue(memoryValue);
        obj.setDiskRemainRate(diskRemainRate);
        // 磁盘剩余
        cacheNum = cluster.getDiskTotalSize();
        cacheMap = CalcUtils.divDataDoubleMap(stat.getPartitionRemainSizeMap(), cacheNum);
        obj.setDiskRemainValue(cacheMap);
        // 设置磁盘剩余sizemap
        obj.setLocalDiskRemainRate(calcRemainRate(stat.getPartitionRemainSizeMap(), stat.getPartitionTotalSizeMap()));
        obj.setLocalRemainSizeValue(stat.getPartitionRemainSizeMap());
        obj.setLocalSizeValue(stat.getPartitionTotalSizeMap());
        // 磁盘读
        cacheNum = cluster.getDiskReadMaxSpeed();
        cacheMap = CalcUtils.divDiffDataDoubleMap(stat.getPartitionReadSpeedMap(), cacheNum);
        obj.setDiskReadValue(cacheMap);
        // 磁盘写
        cacheNum = cluster.getDiskWriteMaxSpeed();
        cacheMap = CalcUtils.divDiffDataDoubleMap(stat.getPartitionWriteSpeedMap(), cacheNum);
        obj.setDiskWriteValue(cacheMap);
        // 网卡接收
        cacheNum = cluster.getNetRxMaxSpeed();
        double netRS = (double) stat.getNetRSpeed() / cacheNum;
        obj.setNetRxValue(netRS);
        // 网卡发送
        cacheNum = cluster.getNetTxMaxSpeed();
        double netTS = stat.getNetTSpeed() / cacheNum;
        obj.setNetTxValue(netTS);
        obj.setStorageNameOnPartitionMap(stat.getStorageNameOnPartitionMap());
        return obj;
    }

    public static Map<String, Double> calcRemainRate(Map<String, Long> remain, Map<String, Long> total) {
        Map<String, Double> doubleMap = new HashMap<>();
        if (remain == null || remain.isEmpty() || total == null || total.isEmpty()) {
            return doubleMap;
        }
        String mount = null;
        long totalSize = 0L;
        long remainSize = 0L;
        double rate = 0.0;
        for (Map.Entry<String, Long> entry : total.entrySet()) {
            mount = entry.getKey();
            totalSize = entry.getValue();
            if (totalSize == 0) {
                continue;
            }
            remainSize = remain.get(mount);
            rate = (double) remainSize / totalSize;
            doubleMap.put(mount, rate);
        }
        return doubleMap;
    }

    /**
     * 概述：匹配sn与分区
     *
     * @param snList      sn目录信息
     * @param mountPoints 挂载点目录信息
     *
     * @return
     *
     * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
     */
    @Deprecated
    public static Map<String, String> matchSnToPatition(Collection<String> snList, Collection<String> mountPoints,
                                                        String dataDir) {
        Map<String, String> objMap = new ConcurrentHashMap<String, String>();
        if (snList == null || mountPoints == null) {
            return objMap;
        }
        // 获取每个sn对应的空间大小
        String mountPoint = null;
        String path = null;
        // 匹配sn与挂载点
        for (String sn : snList) {
            path = dataDir + File.separator + sn;
            mountPoint = DiskUtils.selectPartOfDisk(path, mountPoints);
            if (BrStringUtils.isEmpty(mountPoint)) {
                continue;
            }
            if (!objMap.containsKey(sn)) {
                objMap.put(sn, mountPoint);
            }
        }
        return objMap;
    }

}
