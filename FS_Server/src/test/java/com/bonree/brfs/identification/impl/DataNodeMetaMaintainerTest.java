package com.bonree.brfs.identification.impl;

import com.bonree.brfs.common.ZookeeperPaths;
import com.bonree.brfs.common.resource.ResourceCollectionInterface;
import com.bonree.brfs.common.resource.vo.DataNodeMetaModel;
import com.bonree.brfs.common.resource.vo.LocalPartitionInfo;
import com.bonree.brfs.common.resource.vo.NodeStatus;
import com.bonree.brfs.common.utils.JsonUtils;
import com.bonree.brfs.resource.impl.SigarGather;
import java.util.HashMap;
import java.util.Map;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryNTimes;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataNodeMetaMaintainerTest {
    private static final Logger LOG = LoggerFactory.getLogger(DataNodeMetaMaintainerTest.class);
    private CuratorFramework client;
    private String zkAddress = "localhost:2181";
    private String custerName = "idea";
    private ZookeeperPaths zkpath;
    private ResourceCollectionInterface gather;

    @Before
    public void init() throws Exception {
        this.client = CuratorFrameworkFactory.newClient(zkAddress, new RetryNTimes(100, 1000));
        this.client.start();
        this.client.blockUntilConnected();
        zkpath = ZookeeperPaths.create(custerName, client);
        gather = new SigarGather();

    }

    @Test
    public void constructerTest() throws Exception {
        String host = "192.168.150.237";
        int port = 19999;
        DataNodeMetaMaintainer maintainer = new DataNodeMetaMaintainer(client, gather, zkpath, host, port);
    }

    @Test
    public void getTest() throws Exception {
        String host = "192.168.150.237";
        int port = 19999;
        DataNodeMetaMaintainer maintainer = new DataNodeMetaMaintainer(client, gather, zkpath, host, port);
        DataNodeMetaModel node = maintainer.getDataNodeMeta();
        String content = JsonUtils.toJsonString(node);
        LOG.info("empty --> {}\n json: {}", node, content);
        DataNodeMetaModel new1 = JsonUtils.toObject(content, DataNodeMetaModel.class);
    }

    @Test
    public void createTest() throws Exception {
        String host = "192.168.150.237";
        int port = 19999;
        String serverID = "10";
        String parContent = "{\"partitionGroup\":\"partition_group\","
            + "\"partitionId\":\"40\",\"devName\":\"/dev/mapper/cl-data\","
            + "\"mountPoint\":\"/data\",\"dataDir\":\"/data/br/brfs/data\","
            + "\"totalSize\":2.12342268E8}";
        LocalPartitionInfo local = JsonUtils.toObject(parContent, LocalPartitionInfo.class);
        Map<String, LocalPartitionInfo> map = new HashMap<>();
        map.put(local.getPartitionId(), local);
        DataNodeMetaMaintainer maintainer = new DataNodeMetaMaintainer(client, gather, zkpath, host, port);
        DataNodeMetaModel node = maintainer.getDataNodeMeta();
        node.setServerID(serverID);
        node.setPartitionInfoMap(map);
        node.setStatus(NodeStatus.NORMAL);
        maintainer.updateDataNodeMeta(node);
        DataNodeMetaModel metaModel = maintainer.getDataNodeMeta();
        LOG.info("{}", metaModel);
    }
}
