package com.bonree.brfs.resource.command;

import com.bonree.brfs.common.ZookeeperPaths;
import com.bonree.brfs.common.resource.ResourceCollectionInterface;
import com.bonree.brfs.common.resource.vo.DataNodeMetaModel;
import com.bonree.brfs.common.resource.vo.DiskPartitionInfo;
import com.bonree.brfs.common.resource.vo.DiskPartitionStat;
import com.bonree.brfs.common.resource.vo.LocalPartitionInfo;
import com.bonree.brfs.common.resource.vo.NetInfo;
import com.bonree.brfs.common.resource.vo.NodeStatus;
import com.bonree.brfs.common.resource.vo.NodeVersion;
import com.bonree.brfs.common.utils.JsonUtils;
import com.bonree.brfs.resource.command.identification.DiskNodeIDImpl;
import com.bonree.brfs.resource.impl.SigarGather;
import com.google.common.base.Charsets;
import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import com.google.common.io.Files;
import io.airlift.airline.Command;
import io.airlift.airline.Option;
import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.leader.LeaderSelector;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListener;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.retry.RetryNTimes;
import org.apache.curator.utils.ZKPaths;
import org.apache.zookeeper.CreateMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * V1 环境部署V2 的兼容命令
 * 命令执行，需要一期的数据路径，所以在配置文件中，要保留一期的目录配置项，同时在二期的数据目录配置中有对应的配置，同时集群名要保正一期二期一致
 */
@Command(name = "ids", description = "version 2 upgrade deployment id file comparibility problem solving command")
public class IdsCommand implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(IdsCommand.class);
    @Option(name = "-i", description = "disknode_id file path", required = true)
    private String idsPath;

    @Option(name = "-c", description = "config_file file path", required = true)
    private String configPath;

    @Override
    public void run() {
        try {
            String firstServer = getFirstId(idsPath);
            LOG.info("load id file content: [{}]", firstServer);
            MinDeployModel config = getConfig(configPath);
            LOG.info("load config content: [{}]", config);
            String zkAddresses = config.getZkAddress();
            String clusterName = config.getClustername();
            String partitionGroup = config.getPartitionGroup();
            String ip = config.getIp();
            int port = config.getPort();
            String dataDir = config.getDataDir();
            CuratorFramework client = createCuratorClient(zkAddresses);
            ZookeeperPaths zookeeperPaths = ZookeeperPaths.getBasePath(clusterName, client);
            ResourceCollectionInterface gather = new SigarGather();
            // V1 的数据目录 转换为磁盘id
            LocalPartitionInfo local = gahterLocalPartition(client, gather, zookeeperPaths, dataDir, partitionGroup);
            DataNodeMetaModel model = gatherDataNodeMetaModel(gather, local, firstServer, ip, port);
            // 注册磁盘id
            setDataMeta(client, zookeeperPaths, model);
            // 转换二级serverID目录树
            mvSecondIDTree(client, zookeeperPaths, local, firstServer);
            // 移动路由规则到V2路径中
            mvRoutePath(client, zookeeperPaths, firstServer);
        } catch (Exception e) {
            System.err.println("run idsCommand happen error content: " + e.getMessage());
            LOG.error("run update ids command happen error", e);
        } finally {
            System.exit(0);
        }
    }

    public void mvRoutePath(CuratorFramework client, ZookeeperPaths zkPaths, String firtServer) throws Exception {
        if (client.checkExists().forPath(zkPaths.getBaseLocksPath()) == null) {
            client.create()
                  .creatingParentsIfNeeded()
                  .withMode(CreateMode.PERSISTENT)
                  .forPath(zkPaths.getBaseLocksPath());
        }
        String lockPath = ZKPaths.makePath(zkPaths.getBaseLocksPath(), "deploy");
        LeaderSelector leaderSelector = new LeaderSelector(client, lockPath, new LeaderSelectorListener() {
            @Override
            public void takeLeadership(CuratorFramework client) throws Exception {
                LOG.info("start convert route ...");
                moveDir(client, zkPaths.getBaseRoutePath(), zkPaths.getBaseV2RoutePath());
                LOG.info("convert route successfull");
            }

            private void moveDir(CuratorFramework client, String source, String dent) throws Exception {
                if (client.checkExists().forPath(source) == null) {
                    return;
                }
                byte[] data = client.getData().forPath(source);
                if (data != null && data.length != 0) {
                    if (client.checkExists().forPath(dent) == null) {
                        client.create()
                              .creatingParentsIfNeeded()
                              .withMode(CreateMode.PERSISTENT)
                              .forPath(dent, data);
                        LOG.info("move route to dent : {}", dent);
                    } else {
                        LOG.info("skip route {} ", dent);
                    }
                }
                List<String> childs = client.getChildren().forPath(source);
                if (childs == null || childs.isEmpty()) {
                    return;
                }
                for (String child : childs) {
                    String source1 = ZKPaths.makePath(source, child);
                    String dent1 = ZKPaths.makePath(dent, child);
                    moveDir(client, source1, dent1);
                }
            }

            @Override
            public void stateChanged(CuratorFramework curatorFramework, ConnectionState connectionState) {
                LOG.info("other worker is mv route !! skip it ");
            }
        });
        leaderSelector.setId(firtServer);
        leaderSelector.start();
        // 等选择则主节点并操作完成
        LOG.info("wait leader ship !! will sleep 5 s..");
        Thread.sleep(5000);
    }

    public void mvSecondIDTree(

        CuratorFramework client, ZookeeperPaths zkPath, LocalPartitionInfo partition, String firstServer) throws Exception {
        LOG.info("start mv secondId tree");
        String dentBasepath = ZKPaths.makePath(zkPath.getBaseV2SecondIDPath(), partition.getPartitionId());
        String sourceBasePath = ZKPaths.makePath(zkPath.getBaseServerIdPath(), firstServer);
        if (client.checkExists().forPath(sourceBasePath) == null) {
            LOG.info("no serverid need to convert");
            return;
        }
        List<String> childs = client.getChildren().forPath(sourceBasePath);
        if (childs == null || childs.isEmpty()) {
            LOG.info("no server ids");
            return;
        }
        if (client.checkExists().forPath(dentBasepath) == null) {
            client.create()
                  .creatingParentsIfNeeded()
                  .withMode(CreateMode.PERSISTENT)
                  .forPath(dentBasepath, firstServer.getBytes());
        }
        for (String storageRegionId : childs) {
            String source = ZKPaths.makePath(sourceBasePath, storageRegionId);
            String dent = ZKPaths.makePath(dentBasepath, storageRegionId);
            LOG.info("StorageRegion :[" + storageRegionId + "] converto");
            mvData(client, source, dent);
        }
        LOG.info("mv secondId tree successfull");
    }

    private void mvData(CuratorFramework client, String source, String dent) throws Exception {
        if (client.checkExists().forPath(source) == null) {
            return;
        }
        byte[] data = client.getData().forPath(source);
        if (data == null || data.length == 0) {
            client.create()
                  .creatingParentsIfNeeded()
                  .withMode(CreateMode.PERSISTENT)
                  .forPath(dent);
        } else {
            client.create()
                  .creatingParentsIfNeeded()
                  .withMode(CreateMode.PERSISTENT)
                  .forPath(dent, data);
        }
    }

    public void setDataMeta(CuratorFramework client, ZookeeperPaths zkPath, DataNodeMetaModel model) throws Exception {
        LOG.info("start convert data dir");
        int code = model.getMac().hashCode();
        int port = model.getPort();
        String nodePath = ZKPaths.makePath(zkPath.getBaseDataNodeMetaPath(), code + "_" + port);
        byte[] data = JsonUtils.toJsonBytes(model);
        if (client.checkExists().forPath(nodePath) == null) {
            client.create()
                  .creatingParentsIfNeeded()
                  .withMode(CreateMode.PERSISTENT)
                  .forPath(nodePath, data);
        } else {
            client.setData()
                  .forPath(nodePath, data);
        }
        LOG.info("convert data dir successfull");
    }

    public DataNodeMetaModel gatherDataNodeMetaModel(
        ResourceCollectionInterface gather, LocalPartitionInfo local, String firstServer, String ip, int port) throws Exception {
        DataNodeMetaModel model = new DataNodeMetaModel();

        Map<String, LocalPartitionInfo> map = new HashMap<>();
        map.put(local.getPartitionId(), local);
        model.setPartitionInfoMap(map);

        NetInfo netInfo = gather.collectSingleNetInfo(ip);
        model.setServerID(firstServer);
        model.setIp(ip);
        model.setMac(netInfo.getHwaddr());
        model.setPort(port);
        model.setStatus(NodeStatus.NORMAL);
        model.setVersion(NodeVersion.V2);
        return model;
    }

    public LocalPartitionInfo gahterLocalPartition(CuratorFramework client, ResourceCollectionInterface gather,
                                                   ZookeeperPaths zkPath,
                                                   String dir, String partitionGroup) throws Exception {
        File file = new File(dir);
        DiskPartitionInfo fs = gather.collectSinglePartitionInfo(file.getAbsolutePath());
        if (fs == null) {
            throw new RuntimeException("dir [" + dir + "] can't find vaild partition !!");
        }
        DiskPartitionStat usage = gather.collectSinglePartitionStats(file.getAbsolutePath());
        LocalPartitionInfo local = packageLocalPartitionInfo(fs, usage, dir, partitionGroup);
        String partitionId = getPartitionId(client, zkPath);
        local.setPartitionId(partitionId);
        return local;
    }

    private String getPartitionId(CuratorFramework client, ZookeeperPaths zkPaths) throws Exception {
        return new DiskNodeIDImpl(client, zkPaths.getBaseServerIdSeqPath(), zkPaths.getBaseV2SecondIDPath()).genLevelID();
    }

    /**
     * 封装localpartitioninfo信息
     *
     * @param info
     * @param stat
     * @param dataPath
     *
     * @return
     */
    private LocalPartitionInfo packageLocalPartitionInfo(DiskPartitionInfo info, DiskPartitionStat stat, String dataPath,
                                                         String partitionGroup) {
        LocalPartitionInfo local = new LocalPartitionInfo();
        local.setPartitionGroup(partitionGroup);
        local.setDataDir(dataPath);
        local.setMountPoint(info.getDirName());
        local.setDevName(info.getDevName());
        local.setTotalSize(stat.getTotal());
        return local;
    }

    private CuratorFramework createCuratorClient(String zkAddress) throws Exception {
        CuratorFramework client = CuratorFrameworkFactory
            .newClient(zkAddress, new RetryNTimes(10, 1000));
        client.start();
        client.blockUntilConnected(30, TimeUnit.SECONDS);
        return client;
    }

    /**
     * 获取主机serverid
     *
     * @param idsPath
     *
     * @return
     *
     * @throws Exception
     */
    protected String getFirstId(String idsPath) throws Exception {
        File idFile = new File(idsPath);
        return Files.asCharSource(idFile, Charsets.UTF_8).readFirstLine();
    }

    /**
     * 获取配置项
     *
     * @param configPath
     *
     * @return
     *
     * @throws Exception
     */
    public MinDeployModel getConfig(String configPath) throws Exception {
        Properties prop = new Properties();
        prop.load(new BufferedInputStream(new FileInputStream(new File(configPath))));
        String clusterName = prop.getProperty("cluster.name");
        String zkAddress = prop.getProperty("zookeeper.addresses");
        String dataRoot = prop.getProperty("datanode.data.root");
        String partitionGroup = prop.getProperty("partition.group", "partition_group");
        String ip = prop.getProperty("datanode.service.host");
        int port = Integer.parseInt(prop.getProperty("datanode.service.port", "8881"));

        Preconditions.checkNotNull(clusterName, "config item [cluster.name] is null");
        Preconditions.checkNotNull(zkAddress, "config item [zookeeper.addresses] is null");
        Preconditions.checkNotNull(dataRoot, "config item [datanode.data.root] is null");
        Preconditions.checkNotNull(ip, "config item [datanode.service.host] is null");
        MinDeployModel model = new MinDeployModel(clusterName, zkAddress, dataRoot);
        model.setPartitionGroup(partitionGroup);
        model.setIp(ip);
        model.setPort(port);
        return model;
    }

    protected static class MinDeployModel {
        /**
         * 集群名称
         */
        private String clustername;
        /**
         * 集群使用的zk地址
         */
        private String zkAddress;
        /**
         * 数据存储目录
         */
        private String dataDir;

        /**
         * 磁盘分区名称
         */
        private String partitionGroup = "";

        private String ip;
        private int port;

        public MinDeployModel(String clustername, String zkAddress, String dataDir) {
            this.clustername = clustername;
            this.zkAddress = zkAddress;
            this.dataDir = dataDir;
        }

        public String getClustername() {
            return clustername;
        }

        public void setClustername(String clustername) {
            this.clustername = clustername;
        }

        public String getZkAddress() {
            return zkAddress;
        }

        public void setZkAddress(String zkAddress) {
            this.zkAddress = zkAddress;
        }

        public String getDataDir() {
            return dataDir;
        }

        public void setDataDir(String dataDir) {
            this.dataDir = dataDir;
        }

        public String getPartitionGroup() {
            return partitionGroup;
        }

        public void setPartitionGroup(String partitionGroup) {
            this.partitionGroup = partitionGroup;
        }

        public String getIp() {
            return ip;
        }

        public void setIp(String ip) {
            this.ip = ip;
        }

        public int getPort() {
            return port;
        }

        public void setPort(int port) {
            this.port = port;
        }

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(this)
                              .add("clustername", clustername)
                              .add("zkAddress", zkAddress)
                              .add("dataDir", dataDir)
                              .add("partitionGroup", partitionGroup)
                              .add("ip", ip)
                              .add("port", port)
                              .toString();
        }
    }

    public String getIdsPath() {
        return idsPath;
    }

    public void setIdsPath(String idsPath) {
        this.idsPath = idsPath;
    }

    public String getConfigPath() {
        return configPath;
    }

    public void setConfigPath(String configPath) {
        this.configPath = configPath;
    }
}
