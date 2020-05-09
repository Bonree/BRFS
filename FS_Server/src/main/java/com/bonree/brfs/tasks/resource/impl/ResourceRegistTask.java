package com.bonree.brfs.tasks.resource.impl;

import com.bonree.brfs.common.utils.TimeUtils;
import com.bonree.brfs.disknode.ResourceConfig;
import com.bonree.brfs.resource.ResourceGatherInterface;
import com.bonree.brfs.resource.ResourceRegisterInterface;
import com.bonree.brfs.resource.vo.ResourceModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ResourceRegistTask extends SuperResourceTask {
    private static final Logger LOG = LoggerFactory.getLogger(ResourceRegistTask.class);
    private ResourceGatherInterface gather;
    private ResourceRegisterInterface register;
    private int count = 0;

    public ResourceRegistTask(ResourceGatherInterface gather,
                              ResourceRegisterInterface register, ResourceConfig config) {
        super(LOG, config.getIntervalTime());
        this.gather = gather;
        this.register = register;
    }

    @Override
    protected void atomRun() {
        try {
            ResourceModel model = gather.gatherClusterResource();
            if (model == null) {
                LOG.warn("gather resource is empty !! will upload next time !!");
                return;
            }
            register.registerResource(model);
            if (count < 10) {
                LOG.info("gather resource [{}] successfull !! show per remain time {}",
                         TimeUtils.formatTimeStamp(System.currentTimeMillis(), "yyyy-MM-dd HH:mm:ss"), 9 - count);
            } else if (count % 100 == 0) {
                LOG.info("gather resource [{}] successfull !! count time {}",
                         TimeUtils.formatTimeStamp(System.currentTimeMillis(), "yyyy-MM-dd HH:mm:ss"), count);
            }
            count = count + 1;
        } catch (Exception e) {
            LOG.error("gather resource happen error !!", e);
        }
    }
}
