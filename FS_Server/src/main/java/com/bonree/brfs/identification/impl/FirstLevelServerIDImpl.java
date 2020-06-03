package com.bonree.brfs.identification.impl;

import com.bonree.brfs.common.ZookeeperPaths;
import com.bonree.brfs.common.resource.vo.DataNodeMetaModel;
import com.bonree.brfs.common.resource.vo.NodeStatus;
import com.bonree.brfs.disknode.IDConfig;
import com.bonree.brfs.identification.DataNodeMetaMaintainerInterface;
import com.bonree.brfs.identification.LevelServerIDGen;
import java.util.Collection;
import javax.inject.Inject;
import org.apache.commons.lang3.StringUtils;
import org.apache.curator.framework.CuratorFramework;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*******************************************************************************
 * 版权信息：博睿宏远科技发展有限公司
 * Copyright: Copyright (c) 2007博睿宏远科技发展有限公司,Inc.All Rights Reserved.
 *
 * @date 2020年5月29日 16:19:46
 * @Author: <a href=mailto:zhucg@bonree.com>朱成岗</a>
 * @Description: 1级serverID实例
 ******************************************************************************/
public class FirstLevelServerIDImpl {
    private static final Logger LOG = LoggerFactory.getLogger(FirstLevelServerIDImpl.class);

    private LevelServerIDGen firstServerIDGen;

    private CuratorFramework client;

    private String firstZKPath;

    private String firstServer = null;
    private DataNodeMetaMaintainerInterface maintainer;

    public FirstLevelServerIDImpl(CuratorFramework client, DataNodeMetaMaintainerInterface mainter, String firstZKPath,
                                  String seqPath,
                                  String zkMetaPath) {
        this.client = client;
        this.firstZKPath = firstZKPath;
        this.maintainer = mainter;
        firstServerIDGen = new FirstServerIDGenImpl(client, seqPath);
        initOrLoadServerID();
    }

    @Inject
    public FirstLevelServerIDImpl(
        CuratorFramework client, DataNodeMetaMaintainerInterface mainter, ZookeeperPaths path, IDConfig idConfig) {
        this(client,
             mainter,
             path.getBaseServerIdPath(),
             path.getBaseSequencesPath(),
             path.getBaseDataNodeMetaPath());
    }

    /**
     * 概述：加载一级ServerID
     * 一级ServerID是用于标识每个服务的，不同的服务的一级ServerID一定是不同的，
     * 所以不会出现线程安全的问题
     *
     * @return
     *
     * @user <a href=mailto:weizheng@bonree.com>魏征</a>
     */
    public synchronized String initOrLoadServerID() {
        if (StringUtils.isEmpty(this.firstServer)) {
            this.firstServer = loadFirstServerId();
        }
        return this.firstServer;
    }

    private String loadFirstServerId() {
        String firstServerID = null;
        firstServerID = getFirstID();
        if (firstServerID != null) {
            return firstServerID;
        }
        try {
            Collection<String> first = maintainer.getExistFirst();
            do {
                firstServerID = firstServerIDGen.genLevelID();
            } while (first.contains(firstServerID));
            // client.create()
            //       .creatingParentContainersIfNeeded()
            //       .withMode(CreateMode.PERSISTENT)
            //       .forPath(ZKPaths.makePath(firstZKPath, firstServerID));
            setFirstServer(firstServerID);

        } catch (Exception e) {
            LOG.error("can not persist server id [{}]", firstServerID, e);
            throw new RuntimeException("can not persist server id", e);
        }
        return firstServerID;
    }

    /**
     * 从zk获取id信息
     *
     * @return
     */
    private String getFirstID() {
        try {
            DataNodeMetaModel model = this.maintainer.getDataNodeMeta();
            NodeStatus status = model.getStatus();
            switch (status) {
            case EMPTY:
            case ONLY_PARTITION:
                return null;
            case NORMAL:
            case ONLY_SERVER:
                return model.getServerID();
            default:
                return null;
            }
        } catch (Exception e) {
            throw new RuntimeException("load id info from zk happen error", e);
        }
    }

    private void setFirstServer(String firstServer) throws Exception {
        DataNodeMetaModel model = this.maintainer.getDataNodeMeta();
        NodeStatus status = model.getStatus();
        switch (status) {
        case ONLY_PARTITION:
            model.setServerID(firstServer);
            model.setStatus(NodeStatus.NORMAL);
            break;
        case EMPTY:
            model.setServerID(firstServer);
            model.setStatus(NodeStatus.ONLY_SERVER);
            break;
        case NORMAL:
        default:
            String zkFirst = model.getServerID();
            if (!firstServer.equals(zkFirst)) {
                throw new RuntimeException("find different first id zk:[" + zkFirst + "],apply:[" + firstServer + "]");
            } else {
                return;
            }
        }
        this.maintainer.updateDataNodeMeta(model);
    }
}
