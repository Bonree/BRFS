package com.bonree.brfs.resource.command;

import com.bonree.brfs.common.ZookeeperPaths;
import com.bonree.brfs.common.resource.ResourceCollectionInterface;
import com.bonree.brfs.common.resource.vo.DataNodeMetaModel;
import com.bonree.brfs.common.resource.vo.LocalPartitionInfo;
import com.bonree.brfs.common.utils.FileUtils;
import com.bonree.brfs.resource.impl.SigarGather;
import com.google.common.io.Files;
import java.io.File;
import java.io.FileOutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryNTimes;
import org.apache.curator.utils.ZKPaths;
import org.apache.zookeeper.CreateMode;
import org.junit.Before;
import org.junit.Test;

public class IdsCommandTest {
    private CuratorFramework client = null;
    private String zkAddress = "192.168.150.236:2181";

    // @Before
    public void init() throws Exception {
        client = CuratorFrameworkFactory.newClient(zkAddress, new RetryNTimes(10, 1000));
        client.start();
        client.blockUntilConnected();
    }

    private String create() throws Exception {
        String filePath = this.getClass().getResource("/").getPath() + "/config";
        Properties prop = new Properties();
        prop.setProperty("cluster.name", "idea");
        prop.setProperty("zookeeper.addresses", "192.168.150.236:2181");
        prop.setProperty("datanode.data.root", "/data");
        prop.setProperty("partition.group", "partition_group");
        prop.setProperty("datanode.service.host", "192.168.150.237");
        prop.setProperty("datanode.service.port", "8881");
        prop.store(new FileOutputStream(new File(filePath)), "utf-8");
        return filePath;
    }

    @Test
    public void loadConfigTest() throws Exception {
        String path = create();
        IdsCommand.MinDeployModel config = new IdsCommand().getConfig(path);
        System.out.println(config);
        FileUtils.deleteFile(path);
    }

    private String createIdsFile() throws Exception {
        String filePath = this.getClass().getResource("/").getPath() + "/disknode";
        Files.write("10".getBytes(), new File(filePath));
        return filePath;
    }

    @Test
    public void loadIdsTest() throws Exception {
        String idsPath = createIdsFile();
        System.out.println(new IdsCommand().getFirstId(idsPath));
        FileUtils.deleteFile(idsPath);
    }

    @Test
    public void gatherLocalPartition() throws Exception {
        IdsCommand deploy = new IdsCommand();
        String config = create();
        String ids = createIdsFile();
        IdsCommand.MinDeployModel model = deploy.getConfig(config);
        ZookeeperPaths zkPath = ZookeeperPaths.getBasePath(model.getClustername(), client);
        LocalPartitionInfo partition =
            deploy.gahterLocalPartition(client, new SigarGather(), zkPath, model.getDataDir(), model.getPartitionGroup());
        System.out.println(partition);
        FileUtils.deleteFile(config);
        FileUtils.deleteFile(ids);
    }

    @Test
    public void gatherDataMetaTest() throws Exception {
        IdsCommand deploy = new IdsCommand();
        String config = create();
        String ids = createIdsFile();
        IdsCommand.MinDeployModel model = deploy.getConfig(config);
        ZookeeperPaths zkPath = ZookeeperPaths.getBasePath(model.getClustername(), client);
        ResourceCollectionInterface gather = new SigarGather();
        String firstId = deploy.getFirstId(ids);
        LocalPartitionInfo partition =
            deploy.gahterLocalPartition(client, gather, zkPath, model.getDataDir(), model.getPartitionGroup());
        DataNodeMetaModel meta = deploy.gatherDataNodeMetaModel(gather, partition, firstId, model.getIp(), model.getPort());
        System.out.println(meta);
        FileUtils.deleteFile(config);
        FileUtils.deleteFile(ids);
    }

    @Test
    public void updateDataMetaTest() throws Exception {
        IdsCommand deploy = new IdsCommand();
        String config = create();
        String ids = createIdsFile();
        IdsCommand.MinDeployModel model = deploy.getConfig(config);
        ZookeeperPaths zkPath = ZookeeperPaths.getBasePath(model.getClustername(), client);
        ResourceCollectionInterface gather = new SigarGather();
        LocalPartitionInfo partition =
            deploy.gahterLocalPartition(client, gather, zkPath, model.getDataDir(), model.getPartitionGroup());
        String firstId = deploy.getFirstId(ids);
        DataNodeMetaModel meta = deploy.gatherDataNodeMetaModel(gather, partition, firstId, model.getIp(), model.getPort());
        deploy.setDataMeta(client, zkPath, meta);
        FileUtils.deleteFile(config);
        FileUtils.deleteFile(ids);
    }

    private void createSecondV1Tree(CuratorFramework client, ZookeeperPaths zkPath, String firstID)
        throws Exception {
        String firstPath = ZKPaths.makePath(zkPath.getBaseServerIdPath(), firstID);
        for (int i = 20; i < 29; i++) {
            String path = ZKPaths.makePath(firstPath, i % 20 + "");
            if (client.checkExists().forPath(path) == null) {
                client.create()
                      .creatingParentsIfNeeded()
                      .withMode(CreateMode.PERSISTENT)
                      .forPath(path, (i + "").getBytes());
            }
        }

    }

    @Test
    public void updateServer() throws Exception {
        IdsCommand deploy = new IdsCommand();
        String config = create();
        String ids = createIdsFile();
        IdsCommand.MinDeployModel model = deploy.getConfig(config);
        ZookeeperPaths zkPath = ZookeeperPaths.getBasePath(model.getClustername(), client);
        ResourceCollectionInterface gather = new SigarGather();
        LocalPartitionInfo partition =
            deploy.gahterLocalPartition(client, gather, zkPath, model.getDataDir(), model.getPartitionGroup());
        String firstId = deploy.getFirstId(ids);
        createSecondV1Tree(client, zkPath, firstId);
        deploy.mvSecondIDTree(client, zkPath, partition, firstId);
        FileUtils.deleteFile(config);
        FileUtils.deleteFile(ids);
    }

    private void createRoute(CuratorFramework client, ZookeeperPaths zkPath) throws Exception {
        String zkBase = zkPath.getBaseRoutePath();
        String normalPath = ZKPaths.makePath(zkBase, "normal");
        String virtualPath = ZKPaths.makePath(zkBase, "virtual");
        List<String> paths = new ArrayList<>();
        paths.add(normalPath);
        paths.add(virtualPath);
        for (String path : paths) {
            for (int i = 0; i < 10; i++) {
                String snPath = ZKPaths.makePath(path, i + "");
                for (int j = 0; j < 3; j++) {
                    String uuid = UUID.randomUUID().toString();
                    String routePath = ZKPaths.makePath(snPath, uuid);
                    if (client.checkExists().forPath(routePath) == null) {
                        client.create()
                              .creatingParentsIfNeeded()
                              .withMode(CreateMode.PERSISTENT)
                              .forPath(routePath, uuid.getBytes());
                    }
                }
            }
        }
    }

    @Test
    public void updateRoute() throws Exception {
        IdsCommand deploy = new IdsCommand();
        String config = create();
        String ids = createIdsFile();
        IdsCommand.MinDeployModel model = deploy.getConfig(config);
        String firstServer = deploy.getFirstId(ids);
        ZookeeperPaths zkPath = ZookeeperPaths.getBasePath(model.getClustername(), client);
        // createRoute(client, zkPath);
        deploy.mvRoutePath(client, zkPath, firstServer);
        FileUtils.deleteFile(config);
        FileUtils.deleteFile(ids);
    }

    @Test
    public void testCheck() throws Exception {
        IdsCommand deploy = new IdsCommand();
        String config = createTest();
        String ids = createIdsFile();
        deploy.setConfigPath(config);
        deploy.setIdsPath(ids);
        deploy.run();

    }

    private String createTest() throws Exception {
        String filePath = this.getClass().getResource("/").getPath() + "/config";
        Properties prop = new Properties();
        prop.setProperty("cluster.name", "idea");
        prop.setProperty("zookeeper.addresses", "192.168.150.237:2181");
        prop.setProperty("datanode.data.root", "/data/br/brfs/data");
        prop.setProperty("partition.group", "partition_group");
        prop.setProperty("datanode.service.host", "192.168.150.237");
        prop.setProperty("datanode.service.port", "9900");
        prop.store(new FileOutputStream(new File(filePath)), "utf-8");
        return filePath;
    }
}
