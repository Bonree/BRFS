package com.bonree.brfs.resource.gather.impl;

import com.bonree.brfs.common.resource.vo.DiskPartitionInfo;
import com.bonree.brfs.common.resource.vo.DiskPartitionStat;
import com.bonree.brfs.resource.convertor.DiskConvertor;
import com.bonree.brfs.resource.gather.PartitionGather;
import java.util.Collection;
import java.util.HashSet;
import org.hyperic.sigar.FileSystem;
import org.hyperic.sigar.FileSystemMap;
import org.hyperic.sigar.FileSystemUsage;
import org.hyperic.sigar.Sigar;

public class SigarPartitionGather implements PartitionGather {
    private Sigar sigar = null;
    private DiskConvertor convertor;

    public SigarPartitionGather() {

    }

    @Override
    public DiskPartitionInfo gatherDiskPartition(String dir) throws Exception {
        FileSystemMap fsMap = sigar.getFileSystemMap();
        FileSystem fs = fsMap.getFileSystem(dir);
        return this.convertor.convertToPartitionInfo(fs);
    }

    @Override
    public Collection<DiskPartitionInfo> gatherDiskPartitions() throws Exception {
        Collection<DiskPartitionInfo> set = new HashSet<>();
        FileSystem[] fsArray = sigar.getFileSystemList();
        for (FileSystem fs : fsArray) {
            set.add(this.convertor.convertToPartitionInfo(fs));
        }
        return set;
    }

    @Override
    public DiskPartitionStat gatherDiskPartitonStat(String dir) throws Exception {
        FileSystemMap fsMap = sigar.getFileSystemMap();
        FileSystem fs = fsMap.getFileSystem(dir);
        FileSystemUsage fsusage = sigar.getFileSystemUsage(fs.getDirName());
        return this.convertor.convertToPartitionStat(fs, fsusage);
    }

    @Override
    public Collection<DiskPartitionStat> gatherDiskPartitionStats() throws Exception {
        Collection<DiskPartitionStat> set = new HashSet<>();
        FileSystem[] fsArray = sigar.getFileSystemList();
        for (FileSystem fs : fsArray) {
            FileSystemUsage fsusage = sigar.getFileSystemUsage(fs.getDirName());
            set.add(this.convertor.convertToPartitionStat(fs, fsusage));
        }
        return set;
    }

    @Override
    public void start() throws Exception {
        this.sigar = new Sigar();
        this.convertor = new DiskConvertor();
    }

    @Override
    public void stop() throws Exception {
        if (this.sigar != null) {
            this.sigar.close();
        }
    }
}
