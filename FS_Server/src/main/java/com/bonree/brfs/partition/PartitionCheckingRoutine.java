package com.bonree.brfs.partition;

import com.bonree.brfs.common.resource.ResourceCollectionInterface;
import com.bonree.brfs.common.resource.vo.DataNodeMetaModel;
import com.bonree.brfs.common.resource.vo.DiskPartitionInfo;
import com.bonree.brfs.common.resource.vo.DiskPartitionStat;
import com.bonree.brfs.common.resource.vo.LocalPartitionInfo;
import com.bonree.brfs.common.resource.vo.NodeStatus;
import com.bonree.brfs.common.resource.vo.PartitionType;
import com.bonree.brfs.identification.DataNodeMetaMaintainerInterface;
import com.bonree.brfs.identification.LevelServerIDGen;
import com.google.inject.Inject;
import java.io.File;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*******************************************************************************
 * 版权信息： 北京博睿宏远数据科技股份有限公司
 * Copyright (c) 2007-2020 北京博睿宏远数据科技股份有限公司，Inc. All Rights Reserved.
 * @date 2020年03月24日 20:45:10
 * @author: <a href=mailto:zhucg@bonree.com>朱成岗</a>
 * @description: 磁盘分区校验程序，负责对比配置文件和内部文件的区别，当有新的路径 则申请新的磁盘分区id，若存在旧的磁盘分区被去掉，则创建磁盘变更
 ******************************************************************************/

public class PartitionCheckingRoutine {
    private static final Logger LOG = LoggerFactory.getLogger(PartitionCheckingRoutine.class);
    private LevelServerIDGen idGen;
    private ResourceCollectionInterface gather;
    private List<String> storageDirs;
    private String partitionGroup;
    private DataNodeMetaMaintainerInterface maintainer;

    @Inject
    public PartitionCheckingRoutine(LevelServerIDGen idGen, ResourceCollectionInterface gather, List<String> storageDirs,
                                    DataNodeMetaMaintainerInterface maintainer, String partitionGroup) {
        this.idGen = idGen;
        this.storageDirs = storageDirs;
        this.partitionGroup = partitionGroup;
        this.gather = gather;
        this.maintainer = maintainer;
    }

    public Collection<LocalPartitionInfo> checkValidPartition() {
        String[] dirs = storageDirs.toArray(new String[storageDirs.size()]);
        try {
            // 获取已注册过的磁盘分区节点信息
            Map<String, LocalPartitionInfo> innerMap = readIds();
            // 获取有效的磁盘分区，判断是否存在多个目录位于一个磁盘分区的情况，若存在则抛异常。
            Map<String, LocalPartitionInfo> fsMap = collectVaildFileSystem(dirs);

            // 判断是否存在增加的磁盘分区，若存在则申请磁盘id，若为恢复则将exception改为normal
            checkAdd(innerMap, fsMap);

            // 判断是否存在减少磁盘分区，若存在则将其状态标识为exception
            checkLoss(innerMap, fsMap);

            updateIdInfo(innerMap);
            // 返回有效的磁盘分区，该数据将提供给定时线程做定时上报处理
            return innerMap.values();
        } catch (Exception e) {
            throw new RuntimeException("check partition happen error !!", e);
        }
    }

    private synchronized void updateIdInfo(Map<String, LocalPartitionInfo> validMap) throws Exception {
        Collection<LocalPartitionInfo> partitions = validMap.values();
        Map<String, LocalPartitionInfo> newPartitions = new HashMap<>();
        partitions.stream().forEach(partition -> {
            newPartitions.put(partition.getPartitionId() + "", partition);
        });
        DataNodeMetaModel model = maintainer.getDataNodeMeta();
        model.setPartitionInfoMap(newPartitions);
        NodeStatus status = model.getStatus();
        if (NodeStatus.EMPTY.equals(status)) {
            model.setStatus(NodeStatus.ONLY_PARTITION);
        } else if (NodeStatus.ONLY_SERVER.equals(status)) {
            model.setStatus(NodeStatus.NORMAL);
        }
        maintainer.updateDataNodeMeta(model);
    }

    /**
     * 检查是否有新增的磁盘分区。
     *
     * @param innerMap
     * @param validMap
     */
    public void checkAdd(Map<String, LocalPartitionInfo> innerMap, Map<String, LocalPartitionInfo> validMap) {
        if (innerMap == null) {
            innerMap = new HashMap<>();
        }
        Collection<LocalPartitionInfo> addPartions = findAdd(innerMap, validMap);
        if (addPartions == null || addPartions.isEmpty()) {
            return;
        }
        for (LocalPartitionInfo add : addPartions) {
            LocalPartitionInfo local = createPartitionId(validMap.get(add.getDataDir()));
            innerMap.put(local.getDataDir(), local);
        }
    }

    /**
     * 封装本地磁盘信息
     *
     * @return
     */
    public LocalPartitionInfo createPartitionId(LocalPartitionInfo local) {
        try {
            if (local == null) {
                throw new RuntimeException("get invalid partition info ");
            }
            // 无效的磁盘分区无法申请磁盘id
            if (!PartitionGather.isValid(local, gather)) {
                throw new RuntimeException(
                    "Add invalid disk partition ! path:[" + local.getDataDir() + "] devName:[" + local.getDevName() + "]");
            }
            local.setPartitionId(idGen.genLevelID());
            return local;
        } catch (Exception e) {
            throw new RuntimeException("Acquisition error while creating model !! path:[" + local + "]", e);
        }
    }

    private Collection<LocalPartitionInfo> findAdd(Map<String, LocalPartitionInfo> innerMap,
                                                   Map<String, LocalPartitionInfo> validMap) {
        Set<LocalPartitionInfo> adds = new HashSet<>();
        // 若无内部文件，则为所有的磁盘分区申请id
        if (innerMap == null || innerMap.isEmpty()) {
            adds.addAll(validMap.values());
        } else {
            for (String add : validMap.keySet()) {
                if (innerMap.containsKey(add)) {
                    LocalPartitionInfo zkPart = innerMap.get(add);
                    if (PartitionType.EXCEPTION.equals(zkPart.getType())) {
                        zkPart.setType(PartitionType.NORMAL);
                    }
                    continue;
                }
                adds.add(validMap.get(add));
            }
        }
        return adds;
    }

    /**
     * 剔除未配置的磁盘分区，并发布磁盘变更
     * 并对磁盘分区进行检查，若磁盘分区无效则抛出异常。。
     *
     * @param innerMap
     * @param validMap
     */
    public void checkLoss(Map<String, LocalPartitionInfo> innerMap, Map<String, LocalPartitionInfo> validMap) {
        if (innerMap == null || innerMap.isEmpty()) {
            return;
        }
        Iterator<String> iterator = innerMap.keySet().iterator();
        LocalPartitionInfo loss = null;
        while (iterator.hasNext()) {
            String inner = iterator.next();
            if (validMap.containsKey(inner)) {
                continue;
            }
            loss = innerMap.get(inner);
            loss.setType(PartitionType.EXCEPTION);
            LOG.warn("partition is loss [{}]", loss);
        }
    }

    /**
     * 获取已经注册的id文件
     *
     * @return
     */
    public Map<String, LocalPartitionInfo> readIds() throws Exception {

        DataNodeMetaModel meta = this.maintainer.getDataNodeMeta();
        if (NodeStatus.EMPTY.equals(meta.getStatus())) {
            return new HashMap<>();
        }
        Map<String, LocalPartitionInfo> map = meta.getPartitionInfoMap();
        if (map == null || map.isEmpty()) {
            return new HashMap<>();
        }
        Collection<LocalPartitionInfo> partitionInfos = map.values();
        Map<String, LocalPartitionInfo> newMap = new HashMap<>();
        partitionInfos.stream().forEach(
            parition -> {
                newMap.put(parition.getDataDir(), parition);
            }
        );
        return newMap;
    }

    public Map<String, LocalPartitionInfo> collectVaildFileSystem(String[] dataDir) {
        Map<String, LocalPartitionInfo> fsMap = new ConcurrentHashMap<>();
        try {
            Set<String> keepOnlyOne = new HashSet<>();
            DiskPartitionInfo fs;
            for (String dir : dataDir) {
                File file = new File(dir);
                if (!file.exists()) {
                    FileUtils.forceMkdir(file);
                }
                fs = gather.collectSinglePartitionInfo(file.getAbsolutePath());
                if (fs == null) {
                    throw new RuntimeException("dir [" + dir + "] can't find vaild partition !!");
                }
                DiskPartitionStat usage = gather.collectSinglePartitionStats(file.getAbsolutePath());
                String key = StringUtils.join(fs.getDevName(), fs.getDirName(), usage.getTotal());
                if (keepOnlyOne.add(key)) {
                    LocalPartitionInfo local = packageLocalPartitionInfo(fs, usage, file.getAbsolutePath());
                    fsMap.put(file.getAbsolutePath(), local);
                } else {
                    throw new RuntimeException(
                        "The configured directories are on the same disk partition!! dir:[" + dir + "],partition:["
                            + fs.getDirName() + "]");
                }
            }
        } catch (Exception e) {
            throw new RuntimeException("Error checking internal files", e);
        }
        return fsMap;
    }

    /**
     * 封装localpartitioninfo信息
     *
     * @param info
     * @param stat
     * @param dataPath
     *
     * @return
     */
    private LocalPartitionInfo packageLocalPartitionInfo(DiskPartitionInfo info, DiskPartitionStat stat, String dataPath) {
        LocalPartitionInfo local = new LocalPartitionInfo();
        local.setPartitionGroup(this.partitionGroup);
        local.setDataDir(dataPath);
        local.setMountPoint(info.getDirName());
        local.setDevName(info.getDevName());
        local.setTotalSize(stat.getTotal());
        return local;
    }
}
