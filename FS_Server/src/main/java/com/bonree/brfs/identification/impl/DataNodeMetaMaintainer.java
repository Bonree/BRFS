package com.bonree.brfs.identification.impl;

import com.bonree.brfs.common.ZookeeperPaths;
import com.bonree.brfs.common.resource.ResourceCollectionInterface;
import com.bonree.brfs.common.resource.vo.DataNodeMetaModel;
import com.bonree.brfs.common.resource.vo.NetInfo;
import com.bonree.brfs.common.resource.vo.NodeStatus;
import com.bonree.brfs.common.utils.JsonUtils;
import com.bonree.brfs.configuration.Configs;
import com.bonree.brfs.configuration.units.DataNodeConfigs;
import com.bonree.brfs.identification.DataNodeMetaMaintainerInterface;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.utils.ZKPaths;
import org.apache.zookeeper.CreateMode;

public class DataNodeMetaMaintainer implements DataNodeMetaMaintainerInterface {
    private CuratorFramework client;
    private String nodePath;
    private String basePath;
    private String host;
    private String mac;
    private int port;

    public DataNodeMetaMaintainer(CuratorFramework client, ResourceCollectionInterface gather, ZookeeperPaths zkPath, String host,
                                  int port)
        throws Exception {
        this.client = client;
        this.host = host;
        this.port = port;
        NetInfo net = gather.collectSingleNetInfo(host);
        if (net == null) {
            throw new RuntimeException(host + " not collect net info");
        }
        if (!net.getAddress().equals(host)) {
            throw new RuntimeException("gather address not match config ! config: " + host + ",gather: " + net.getAddress());
        }
        mac = net.getHwaddr();
        int code = this.mac.hashCode();
        this.nodePath = ZKPaths.makePath(zkPath.getBaseDataNodeMetaPath(), code + "_" + port);
        this.basePath = zkPath.getBaseDataNodeMetaPath();
    }

    @Inject
    public DataNodeMetaMaintainer(CuratorFramework client, ResourceCollectionInterface gather, ZookeeperPaths zkPath)
        throws Exception {
        this(client, gather, zkPath, Configs.getConfiguration().getConfig(DataNodeConfigs.CONFIG_HOST),
             Configs.getConfiguration().getConfig(DataNodeConfigs.CONFIG_PORT));
    }

    @Override
    public synchronized DataNodeMetaModel getDataNodeMeta() throws Exception {
        if (client.checkExists().forPath(nodePath) == null) {
            return getEmptyMeta();
        }
        byte[] data = client.getData().forPath(nodePath);
        if (data == null || data.length == 0) {
            return getEmptyMeta();
        }
        return JsonUtils.toObjectQuietly(data, DataNodeMetaModel.class);
    }

    @Override
    public void updateDataNodeMeta(DataNodeMetaModel metaData) throws Exception {
        byte[] data = JsonUtils.toJsonBytes(metaData);
        if (data == null || data.length == 0) {
            throw new IllegalStateException("DataNodeMetal is null");
        }
        if (client.checkExists().forPath(nodePath) == null) {
            client.create()
                  .creatingParentsIfNeeded()
                  .withMode(CreateMode.PERSISTENT)
                  .forPath(nodePath, data);
        } else {
            client.setData()
                  .forPath(nodePath, data);
        }
    }

    @Override
    public Collection<String> getExistFirst() throws Exception {
        if (client.checkExists().forPath(basePath) == null) {
            return ImmutableList.of();
        }
        List<String> childs = client.getChildren().forPath(basePath);
        if (childs == null || childs.isEmpty()) {
            return ImmutableList.of();
        }
        Collection<String> set = new HashSet<>();
        for (String child : childs) {
            String zkPath = ZKPaths.makePath(this.basePath, child);
            try {
                byte[] data = client.getData().forPath(zkPath);
                if (data == null || data.length == 0) {
                    continue;
                }
                DataNodeMetaModel model = JsonUtils.toObject(data, DataNodeMetaModel.class);
                NodeStatus status = model.getStatus();
                switch (status) {
                case ONLY_SERVER:
                case NORMAL:
                    String first = model.getServerID();
                    if (StringUtils.isNotEmpty(first)) {
                        set.add(first);
                    }
                    break;
                default:

                }
            } catch (Exception e) {
                throw new IllegalStateException("get data from " + zkPath + "happen error ", e);
            }
        }
        return set;
    }

    private DataNodeMetaModel getEmptyMeta() {
        DataNodeMetaModel model = new DataNodeMetaModel();
        model.setIp(host);
        model.setPort(port);
        model.setMac(mac);
        return model;
    }
}
