package com.bonree.brfs.partition;

import com.bonree.brfs.common.utils.JsonUtils;
import com.bonree.brfs.identification.SecondIdsInterface;
import com.bonree.brfs.partition.model.LocalPartitionInfo;
import com.bonree.brfs.identification.LevelServerIDGen;
import com.google.inject.Inject;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.hyperic.sigar.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;


/*******************************************************************************
 * 版权信息： 北京博睿宏远数据科技股份有限公司
 * Copyright (c) 2007-2020 北京博睿宏远数据科技股份有限公司，Inc. All Rights Reserved.
 * @date 2020年03月24日 20:45:10
 * @author: <a href=mailto:zhucg@bonree.com>朱成岗</a>
 * @description: 磁盘分区校验程序，负责对比配置文件和内部文件的区别，当有新的路径 则申请新的磁盘分区id，若存在旧的磁盘分区被去掉，则创建磁盘变更
 ******************************************************************************/

public class PartitionCheckingRoutine {
    private final static Logger LOG = LoggerFactory.getLogger(PartitionCheckingRoutine.class);
    private LevelServerIDGen idGen;
    // todo 磁盘变更主动发布接口
    //private xxx
    private List<String> dataConfig;
    private String innerDir;
    private String partitionGroup;
    @Inject
    public PartitionCheckingRoutine(LevelServerIDGen idGen, List<String> dataConfig, String innerDir, String partitionGroup) {
        this.idGen = idGen;
        this.dataConfig = dataConfig;
        this.innerDir = innerDir;
        this.partitionGroup = partitionGroup;
    }

    public Collection<LocalPartitionInfo> checkVaildPartition() {
        String[] dirs = dataConfig.toArray(new String[dataConfig.size()]);
        // 获取已注册过的磁盘分区节点信息
        Map<String, LocalPartitionInfo> innerMap = readIds(innerDir);
        // 获取有效的磁盘分区，判断是否存在多个目录位于一个磁盘分区的情况，若存在则抛异常。
        Map<String, FileSystem> fsMap = collectVaildFileSystem(dirs);

        // 剔除未配置的磁盘分区，并发布磁盘变更
        checkLoss(innerMap, fsMap);

        // 判断是否存在增加的磁盘分区，若存在则申请磁盘id
        checkAdd(innerMap, fsMap);

        // 返回有效的磁盘分区，该数据将提供给定时线程做定时上报处理
        return innerMap.values();
    }

    /**
     * 检查是否有新增的磁盘分区。
     * @param innerMap
     * @param validMap
     */
    public void checkAdd(Map<String, LocalPartitionInfo> innerMap, Map<String, FileSystem> validMap) {
        if (innerMap == null) {
            innerMap = new HashMap<>();
        }
        Collection<String> addPartions = findAdd(innerMap, validMap);
        if (addPartions == null || addPartions.isEmpty()) {
            return;
        }
        Sigar sigar = new Sigar();
        try {
            for (String add : addPartions) {
                LocalPartitionInfo local = packageLocal(validMap.get(add), sigar, add);
                innerMap.put(local.getDataDir(), local);
                File idFile = new File(this.innerDir+File.separator+local.getPartitionId());
                try {
                    byte[] data = JsonUtils.toJsonBytesQuietly(local);
                    FileUtils.writeByteArrayToFile(idFile,data);
                } catch (IOException e) {
                    throw new RuntimeException("An error occurred while creating the internal file! path:"+idFile.getAbsolutePath(),e);
                }
            }
        } finally {
            sigar.close();
        }
    }

    /**
     * 封装本地磁盘信息
     * @param fs
     * @param sigar
     * @param add
     * @return
     */
    public LocalPartitionInfo packageLocal(FileSystem fs, Sigar sigar, String add) {
        try {
            LocalPartitionInfo local = new LocalPartitionInfo();
            local.setPartitionGroup(this.partitionGroup);
            local.setDataDir(add);
            local.setMountPoint(fs.getDirName());
            local.setDevName(fs.getDevName());
            FileSystemUsage usage = sigar.getFileSystemUsage(fs.getDirName());
            local.setTotalSize(usage.getTotal());
            // 无效的磁盘分区无法申请磁盘id
            if(!PartitionGather.isValid(local,fs,sigar)){
                throw new RuntimeException("Add invalid disk partition ! path:["+local.getDataDir()+"] devName:["+local.getDevName()+"]");
            }
            local.setPartitionId(idGen.genLevelID());
            return local;
        } catch (SigarException e) {
            throw new RuntimeException("Acquisition error while creating model !! path:[" + add + "]", e);
        }
    }
    private Collection<String> findAdd(Map<String, LocalPartitionInfo> innerMap, Map<String, FileSystem> validMap) {
        Set<String> adds = new HashSet<>();
        // 若无内部文件，则为所有的磁盘分区申请id
        if (innerMap == null||innerMap.isEmpty()) {
            adds.addAll(validMap.keySet());
        } else {
            for (String add : validMap.keySet()) {
                if (innerMap.containsKey(add)) {
                    continue;
                }
                adds.add(add);
            }
        }
        return adds;
    }

    /**
     * 剔除未配置的磁盘分区，并发布磁盘变更
     * 并对磁盘分区进行检查，若磁盘分区无效则抛出异常。。
     * @param innerMap
     * @param validMap
     */
    public void checkLoss(Map<String, LocalPartitionInfo> innerMap, Map<String, FileSystem> validMap) {
        if (innerMap == null || innerMap.isEmpty()) {
            return;
        }
        Iterator<String> iterator = innerMap.keySet().iterator();
        LocalPartitionInfo loss = null;
        while(iterator.hasNext()){
            String inner = iterator.next();
            if(validMap.containsKey(inner)){
                continue;
            }
            loss = innerMap.get(inner);
            iterator.remove();
            // 发布磁盘变更信息后，删除内部文件
            FileUtils.deleteQuietly(new File(this.innerDir+File.separator+loss.getPartitionId()));
            LOG.error("partition is loss [{}]",loss);
        }
    }

    /**
     * 获取本地已经注册的id文件
     *
     * @param path
     * @return
     */
    public Map<String, LocalPartitionInfo> readIds(String path) {
        File file = new File(path);
        if (!file.exists()) {
            try {
                FileUtils.forceMkdir(file);
            } catch (IOException e) {
            throw new RuntimeException("path [" + path + "] is not exists and can't create !!",e);
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
            if(file.equals(f)){
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

    /**
     * 获取配置文件中datadir对应的磁盘分区信息，若存在多个目录在同一个磁盘分区时，将抛异常，若不存在分区目录，则抛异常
     * 当目录不存在时，将抛出异常，因为涉及到多个磁盘分区，所以程序无法自动创建目录
     *
     * @param dataDir
     * @return
     * @throws SigarException
     */
    public Map<String, FileSystem> collectVaildFileSystem(String[] dataDir) {
        Map<String, FileSystem> fsMap = new ConcurrentHashMap<>();
        Sigar sigar = new Sigar();
        try {
            FileSystemMap fileSystemMap = sigar.getFileSystemMap();
            FileSystem fs = null;
            Set<String> keepOnlyOne = new HashSet<>();
            for (String dir : dataDir) {
                File file = new File(dir);
                if(!file.exists()){
                    FileUtils.forceMkdir(file);
                }
                fs = fileSystemMap.getMountPoint(file.getAbsolutePath());
                if (fs == null) {
                    throw new RuntimeException("dir [" + dir + "] can't find vaild partition !!");
                }
                FileSystemUsage usage = sigar.getFileSystemUsage(fs.getDirName());
                String key = StringUtils.join(fs.getDevName(), fs.getDirName(), usage.getTotal());
//                fsMap.put(dir, fs);
                if (keepOnlyOne.add(key)) {
                    fsMap.put(dir, fs);
                } else {
                    throw new RuntimeException("The configured directories are on the same disk partition!! dir:[" + dir + "],partition:[" + fs.getDirName() + "]");
                }
            }
        } catch (SigarException|IOException e) {
            throw new RuntimeException("Error checking internal files", e);
        } finally {
            sigar.close();
        }
        return fsMap;
    }
}
