package com.bonree.brfs.tasks.resource.impl;

import com.bonree.brfs.resource.ResourceGatherInterface;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * GUI 采集资源信息,文件个数的维护，
 */
public class GuiResourcTask extends SuperResourceTask {
    private static final Logger LOG = LoggerFactory.getLogger(GuiResourcTask.class);
    private ResourceGatherInterface gather;
    private String storagePath;
    private int ttl;

    public GuiResourcTask(ResourceGatherInterface gather, String storagePath, int intervalTime,int ttl) {
        super(LOG, intervalTime);
        this.gather = gather;
        this.storagePath = storagePath;
        this.ttl = ttl;
    }

    @Override
    protected void atomRun() {
        long time = getGatherGranuleTime();
    }

    private long getGatherGranuleTime(){
        long time = System.currentTimeMillis();
        return time = time - time%(getIntervalSecond()*1000);
    }
}
