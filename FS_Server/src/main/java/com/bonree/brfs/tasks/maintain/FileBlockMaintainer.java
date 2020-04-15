package com.bonree.brfs.tasks.maintain;

import com.bonree.brfs.identification.LocalPartitionInterface;

import java.util.concurrent.ScheduledExecutorService;

public class FileBlockMaintainer {
    private ScheduledExecutorService pool = null;
    private long intervalTime;
    private LocalPartitionInterface localPartitionInterface;

    public FileBlockMaintainer(LocalPartitionInterface localPartitionInterface, long intervalTime) {
        this.localPartitionInterface = localPartitionInterface;
        this.intervalTime = intervalTime;
    }
}
