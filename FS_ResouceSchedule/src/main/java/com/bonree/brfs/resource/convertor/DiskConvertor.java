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

    public DiskPartitionStat convertToPartitionStat(FileSystem fs, FileSystemUsage net) {
        DiskPartitionStat stat = new DiskPartitionStat();
        stat.setDirName(fs.getDirName());
        stat.setDevName(fs.getDevName());
        stat.setTotal(net.getTotal());
        stat.setFree(net.getFree());
        stat.setUsed(net.getUsed());
        stat.setAvail(net.getAvail());
        stat.setFiles(net.getFiles());
        stat.setFreeFiles(net.getFreeFiles());
        stat.setDiskReads(net.getDiskReads());
        stat.setDiskWrites(net.getDiskWrites());
        stat.setDiskReadBytes(net.getDiskReadBytes());
        stat.setDiskWriteBytes(net.getDiskWriteBytes());
        stat.setDiskQueue(net.getDiskQueue());
        stat.setDiskServiceTime(net.getDiskServiceTime());
        stat.setUsePercent(net.getUsePercent());
        return stat;
    }
}
