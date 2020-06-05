package com.bonree.brfs.resource.convertor;

import com.bonree.brfs.common.resource.vo.DiskPartitionInfo;
import com.bonree.brfs.common.resource.vo.DiskPartitionStat;
import org.hyperic.sigar.FileSystem;
import org.hyperic.sigar.FileSystemUsage;

public class DiskConvertor {
    public DiskPartitionInfo convertToPartitionInfo(FileSystem fs) {
        DiskPartitionInfo info = new DiskPartitionInfo();
        info.setDirName(fs.getDirName());
        info.setDevName(fs.getDevName());
        info.setTypeName(fs.getTypeName());
        info.setSysTypeName(fs.getSysTypeName());
        info.setOptions(fs.getOptions());
        info.setType(fs.getType());
        info.setFlags(fs.getFlags());
        return info;
    }

    public DiskPartitionStat convertToPartitionStat(FileSystem fs, FileSystemUsage usage) {
        DiskPartitionStat stat = new DiskPartitionStat();
        stat.setDirName(fs.getDirName());
        stat.setDevName(fs.getDevName());
        stat.setTotal(usage.getTotal());
        stat.setFree(usage.getFree());
        stat.setUsed(usage.getUsed());
        stat.setAvail(usage.getAvail());
        stat.setFiles(usage.getFiles());
        stat.setFreeFiles(usage.getFreeFiles());
        stat.setDiskReads(usage.getDiskReads());
        stat.setDiskWrites(usage.getDiskWrites());
        stat.setDiskReadBytes(usage.getDiskReadBytes());
        stat.setDiskWriteBytes(usage.getDiskWriteBytes());
        stat.setDiskQueue(usage.getDiskQueue());
        stat.setDiskServiceTime(usage.getDiskServiceTime());
        stat.setUsePercent(usage.getUsePercent());
        return stat;
    }
}
