package com.bonree.brfs.schedulers.jobs.system;

import com.bonree.brfs.common.ZookeeperPaths;
import com.bonree.brfs.duplication.storageregion.StorageRegion;
import com.bonree.brfs.duplication.storageregion.StorageRegionManager;
import com.bonree.brfs.duplication.storageregion.StorageRegionStateListener;
import com.bonree.brfs.duplication.storageregion.impl.DefaultStorageRegionManager;
import com.bonree.brfs.identification.IDSManager;
import com.bonree.brfs.identification.VirtualServerID;
import com.bonree.brfs.identification.impl.VirtualServerIDImpl;
import com.bonree.brfs.schedulers.task.manager.MetaTaskManagerInterface;
import com.bonree.brfs.schedulers.task.manager.impl.DefaultReleaseTask;
import com.bonree.brfs.schedulers.task.model.TaskModel;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryNTimes;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CreateSystemTaskJobTest {
    private static final Logger LOG = LoggerFactory.getLogger(CreateSystemTaskJobTest.class);
    String zkAddress = "192.168.150.236:2181";
    String clusterName = "idea";
    CuratorFramework client = null;
    ZookeeperPaths zkPaths;
    MetaTaskManagerInterface release;
    StorageRegionManager manager;
    VirtualServerID virtualServerID = null;

    @Before
    public void init() throws Exception {
        client = CuratorFrameworkFactory.newClient(zkAddress, new RetryNTimes(10, 1000));
        client.start();
        client.blockUntilConnected();
        zkPaths = ZookeeperPaths.getBasePath(clusterName, client);
        release = new DefaultReleaseTask(client, zkPaths);
        manager = new DefaultStorageRegionManager(client, zkPaths, null);
        manager.start();
        manager.addStorageRegionStateListener(new StorageRegionStateListener() {
            @Override
            public void storageRegionAdded(StorageRegion region) {

            }

            @Override
            public void storageRegionUpdated(StorageRegion region) {

            }

            @Override
            public void storageRegionRemoved(StorageRegion region) {

            }
        });
        virtualServerID = new VirtualServerIDImpl(client, zkPaths);
        Thread.sleep(10000);
    }

    @Test
    public void testCreateVirtualTask() {
        CreateSystemTaskJob job = new CreateSystemTaskJob();
        TaskModel task = job.createVirtualTask(release, virtualServerID, manager);
        LOG.info("--->{}",task);
    }
}
