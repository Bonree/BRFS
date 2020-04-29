package com.bonree.brfs.tasks.resource.impl;

import com.bonree.brfs.resource.ResourceGatherInterface;
import com.bonree.brfs.resource.ResourceRegisterInterface;
import com.bonree.brfs.resource.vo.ResourceModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ResourceRegistTask extends SuperResourceTask {
    private static final Logger LOG = LoggerFactory.getLogger(ResourceRegistTask.class);
    private ResourceGatherInterface gather;
    private ResourceRegisterInterface register;

    public ResourceRegistTask(ResourceGatherInterface gather,
                              ResourceRegisterInterface register,int intervalTime) {
        super(LOG,intervalTime);
        this.gather = gather;
        this.register = register;
    }

    @Override
    protected void atomRun() {
        try {
            ResourceModel model = gather.gatherClusterResource();
            if(model == null){
                LOG.warn("gather resource is empty !! will upload next time !!");
                return;
            }
            register.registerResource(model);
            LOG.info("gather resource successfull !!");
        } catch (Exception e) {
            LOG.error("gather resource happen error !!",e);
        }
    }
}
