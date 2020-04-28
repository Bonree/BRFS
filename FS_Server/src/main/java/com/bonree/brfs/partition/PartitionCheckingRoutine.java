package com.bonree.brfs.partition;

import com.bonree.brfs.common.resource.ResourceCollectionInterface;
import com.bonree.brfs.common.resource.vo.DiskPartitionInfo;
import com.bonree.brfs.common.resource.vo.DiskPartitionStat;
import com.bonree.brfs.common.utils.JsonUtils;
import com.bonree.brfs.identification.LevelServerIDGen;
import com.bonree.brfs.metrics.DiskPartition;
import com.bonree.brfs.partition.model.LocalPartitionInfo;
import com.google.inject.Inject;
import java.io.File;
import java.io.IOException;
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
import org.hyperic.sigar.FileSystem;
import org.hyperic.sigar.FileSystemMap;
import org.hyperic.sigar.FileSystemUsage;
import org.hyperic.sigar.Sigar;
import org.hyperic.sigar.SigarException;
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
    // todo 磁盘变更主动发布接口
    //private xxx
    private List<String> dataConfig;
    private String innerDir;
    private String partitionGroup;

    @Inject
    public PartitionCheckingRoutine(LevelServerIDGen idGen,ResourceCollectionInterface gather, List<String> dataConfig, String innerDir, String partitionGroup) {
        this.idGen = idGen;
        this.dataConfig = dataConfig;
        this.innerDir = innerDir;
        this.partitionGroup = partitionGroup;
        this.gather = gather;
    }

    public Collection<LocalPartitionInfo> checkVaildPartition() {
        String[] dirs = dataConfig.toArray(new String[dataConfig.size()]);
        // 获取已注册过的磁盘分区节点信息
        Map<String, LocalPartitionInfo> innerMap = readIds(innerDir);
        // 获取有效的磁盘分区，判断是否存在多个目录位于一个磁盘分区的情况，若存在则抛异常。
        Map<String, LocalPartitionInfo> fsMap = collectVaildFileSystem(dirs);

        // 剔除未配置的磁盘分区，并发布磁盘变更
        checkLoss(innerMap, fsMap);

        // 判断是否存在增加的磁盘分区，若存在则申请磁盘id
        checkAdd(innerMap, fsMap);

        // 返回有效的磁盘分区，该数据将提供给定时线程做定时上报处理
        return innerMap.values();
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
            LocalPartitionInfo local = createPartitionId(validMap.get(add));
            innerMap.put(local.getDataDir(), local);
            File idFile = new File(this.innerDir + File.separator + local.getPartitionId());
            try {
                byte[] data = JsonUtils.toJsonBytesQuietly(local);
                FileUtils.writeByteArrayToFile(idFile, data);
            } catch (IOException e) {
                throw new RuntimeException(
                    "An error occurred while creating the internal file! path:" + idFile.getAbsolutePath(), e);
            }
        }
    }

    /**
     * 封装本地磁盘信息
     *
     * @return
     */
    public LocalPartitionInfo createPartitionId( LocalPartitionInfo local) {
        try {
            // 无效的磁盘分区无法申请磁盘id
            if (!PartitionGather.isValid(local,gather)) {
                throw new RuntimeException(
                    "Add invalid disk partition ! path:[" + local.getDataDir() + "] devName:[" + local.getDevName() + "]");
            }
            local.setPartitionId(idGen.genLevelID());
            return local;
        } catch (Exception e) {
            throw new RuntimeException("Acquisition error while creating model !! path:[" + local + "]", e);
        }
    }

    private Collection<LocalPartitionInfo> findAdd(Map<String, LocalPartitionInfo> innerMap, Map<String, LocalPartitionInfo> validMap) {
        Set<LocalPartitionInfo> adds = new HashSet<>();
        // 若无内部文件，则为所有的磁盘分区申请id
        if (innerMap == null || innerMap.isEmpty()) {
            adds.addAll(validMap.values());
        } else {
            for (String add : validMap.keySet()) {
                if (innerMap.containsKey(add)) {
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
            iterator.remove();
            // 发布磁盘变更信息后，内部文件不删除，
            //            FileUtils.deleteQuietly(new File(this.innerDir+File.separator+loss.getPartitionId()));
            LOG.warn("partition is loss [{}]", loss);
        }
    }

    /**
     * 获取本地已经注册的id文件
     *
     * @param path
     *
     * @return
     */
    public Map<String, LocalPartitionInfo> readIds(String path) {
        File file = new File(path);
        if (!file.exists()) {
            try {
                FileUtils.forceMkdir(file);
            } catch (IOException e) {
                throw new RuntimeException("path [" + path + "] is not exists and can't create !!", e);
            }
        }
        if (!file.isDirectory()) {
            throw new RuntimeException("path [" + path + "] is not directory !!");
        }
        File[] files = file.listFiles();
        if (files == null || files.length == 0) {
            return new HashMap<>();
        }
        Map<String, LocalPartitionInfo> map = new HashMap<>(files.length);
        LocalPartitionInfo part;
        for (File f : files) {
            if (file.equals(f)) {
                continue;
            }
            part = readByFile(f);
            map.put(part.getDataDir(), part);
        }
        return map;
    }

    /**
     * 读取内部id文件
     *
     * @param file
     *
     * @return
     */
    private LocalPartitionInfo readByFile(File file) {
        if (!file.isFile()) {
            throw new RuntimeException("path [" + file.getAbsolutePath() + "] is not file !!");
        }
        try {
            byte[] data = FileUtils.readFileToByteArray(file);
            if (data == null || data.length == 0) {
                throw new RuntimeException("path [" + file.getAbsolutePath() + "] is empty !!");
            }
            LocalPartitionInfo partitionInfo = JsonUtils.toObject(data, LocalPartitionInfo.class);
            return partitionInfo;
        } catch (IOException e) {
            throw new RuntimeException("path [" + file.getAbsolutePath() + "] read happen error !!", e);
        } catch (JsonUtils.JsonException e) {
            throw new RuntimeException("path [" + file.getAbsolutePath() + "] convert to instance happen error !!", e);
        }
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
                    LocalPartitionInfo local = packageLocalPartitionInfo(fs,usage,file.getAbsolutePath());
                    fsMap.put(dir, local);
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
     * @param info
     * @param stat
     * @param dataPath
     * @return
     */
    private LocalPartitionInfo packageLocalPartitionInfo(DiskPartitionInfo info,DiskPartitionStat stat,String dataPath){
        LocalPartitionInfo local = new LocalPartitionInfo();
        local.setPartitionGroup(this.partitionGroup);
        local.setDataDir(dataPath);
        local.setMountPoint(info.getDirName());
        local.setDevName(info.getDevName());
        local.setTotalSize(stat.getTotal());
        return local;
    }
}
