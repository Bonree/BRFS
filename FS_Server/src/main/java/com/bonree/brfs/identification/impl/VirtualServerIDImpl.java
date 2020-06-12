package com.bonree.brfs.identification.impl;

import com.bonree.brfs.common.ZookeeperPaths;
import com.bonree.brfs.common.sequencenumber.SequenceNumberBuilder;
import com.bonree.brfs.common.sequencenumber.ZkSequenceNumberBuilder;
import com.bonree.brfs.identification.VirtualServerID;
import com.google.common.primitives.Ints;
import com.google.inject.Inject;
import java.util.ArrayList;
import java.util.List;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.utils.ZKPaths;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*******************************************************************************
 * 版权信息：博睿宏远科技发展有限公司
 * Copyright: Copyright (c) 2007博睿宏远科技发展有限公司,Inc.All Rights Reserved.
 *
 * @date 2018年4月13日 下午1:09:46
 * @Author: <a href=mailto:weizheng@bonree.com>魏征</a>
 * @Description: virtual serverID 管理，此处可能需要进行缓存
 ******************************************************************************/
public class VirtualServerIDImpl implements VirtualServerID {
    private static final Logger LOG = LoggerFactory.getLogger(VirtualServerIDImpl.class);

    private static final int STATE_INVALID = 1;
    private static final int STATE_VALID = 2;

    private static final String VIRTUAL_ID_INDEX_NODE = "virtualIdIndex";
    public static final int VIRTUAL_ID_PREFIX = 3;

    private static final String VIRTUAL_ID_CONTAINER = "virtualIdContainer";

    private CuratorFramework client;
    private final String virtualIdContainer;

    private SequenceNumberBuilder virtualServerIDCreator;

    public VirtualServerIDImpl(CuratorFramework client, String basePath) {
        this.client = client;
        this.virtualServerIDCreator = new ZkSequenceNumberBuilder(client, ZKPaths.makePath(basePath, VIRTUAL_ID_INDEX_NODE));
        this.virtualIdContainer = ZKPaths.makePath(basePath, VIRTUAL_ID_CONTAINER);
    }

    @Inject
    public VirtualServerIDImpl(CuratorFramework client, ZookeeperPaths path) {
        this(client, path.getBaseServerIdSeqPath());
    }

    @Override
    public String getVirtualIdContainerPath() {
        return virtualIdContainer;
    }

    private String createVirtualId(int storageId) {
        try {
            int uniqueId = virtualServerIDCreator.nextSequenceNumber();

            StringBuilder idBuilder = new StringBuilder();
            idBuilder.append(VIRTUAL_ID_PREFIX).append(uniqueId);

            String virtualId = idBuilder.toString();
            String nodePath = client.create()
                                    .creatingParentsIfNeeded()
                                    .withMode(CreateMode.PERSISTENT)
                                    .forPath(ZKPaths.makePath(virtualIdContainer, String.valueOf(storageId), virtualId),
                                             Ints.toByteArray(STATE_VALID));

            if (nodePath != null) {
                return virtualId;
            }
        } catch (Exception e) {
            LOG.error("create virtual id node error", e);
        }

        return null;
    }

    @Override
    public List<String> getVirtualID(int storageIndex, int count, List<String> diskFirstIDs) {
        List<String> resultVirtualIds = new ArrayList<String>(count);

        List<String> virtualIds = listValidVirtualIds(storageIndex);

        int index = 0;
        for (int i = 0; index < count && i < virtualIds.size(); i++, index++) {
            resultVirtualIds.add(virtualIds.get(i));
        }

        for (; index < count; index++) {
            resultVirtualIds.add(createVirtualId(storageIndex));
        }
        LOG.info("register disk first level ids:" + diskFirstIDs);
        try {
            for (String diskFirstID : diskFirstIDs) {
                for (String vid : resultVirtualIds) {
                    String registerNode = ZKPaths.makePath(virtualIdContainer, String.valueOf(storageIndex), vid, diskFirstID);
                    if (client.checkExists().forPath(registerNode) == null) {
                        client.create()
                              .creatingParentsIfNeeded()
                              .withMode(CreateMode.PERSISTENT)
                              .forPath(registerNode);
                    }
                }
            }
        } catch (Exception e) {
            LOG.error("register vid error for " + resultVirtualIds, e);
        }
        return resultVirtualIds;
    }

    private boolean updateVirutalIdState(int storageId, String virtualId, int state) {
        String node = ZKPaths.makePath(virtualIdContainer, String.valueOf(storageId), virtualId);

        try {
            Stat stat = client.setData().forPath(node, Ints.toByteArray(state));

            return stat != null;
        } catch (Exception e) {
            LOG.error("invalid virtual id[{}:{}] error", storageId, virtualId, e);
        }

        return false;
    }

    @Override
    public boolean validVirtualId(int storageIndex, String virtualId) {
        return updateVirutalIdState(storageIndex, virtualId, STATE_VALID);
    }

    @Override
    public boolean invalidVirtualId(int storageIndex, String virtualId) {
        return updateVirutalIdState(storageIndex, virtualId, STATE_INVALID);
    }

    @Override
    public boolean deleteVirtualId(int storageIndex, String virtualId) {
        try {
            client.delete().guaranteed().deletingChildrenIfNeeded()
                  .forPath(ZKPaths.makePath(virtualIdContainer, String.valueOf(storageIndex), virtualId));
            return true;
        } catch (Exception e) {
            LOG.error("delete virtual id node[{}:{}] error", storageIndex, virtualId, e);
        }

        return false;
    }

    private List<String> getVirtualIdListByStorageId(int storageId, int state) {
        List<String> result = new ArrayList<String>();
        try {
            String path = ZKPaths.makePath(virtualIdContainer, String.valueOf(storageId));
            List<String> nodeList = client.getChildren().forPath(path);
            if (nodeList != null) {
                for (String node : nodeList) {
                    String nodePath = ZKPaths.makePath(virtualIdContainer, String.valueOf(storageId), node);
                    try {
                        byte[] data = client.getData().forPath(nodePath);
                        if (data == null) {
                            continue;
                        }

                        if ((Ints.fromByteArray(data) & state) > 0) {
                            result.add(node);
                        }
                    } catch (Exception e) {
                        LOG.error("get data of node[{}] error", nodePath, e);
                    }
                }
            }

        } catch (NoNodeException e) {
            //ignore
        } catch (Exception e) {
            LOG.error("get virtual id by sn[{}] node error", storageId, e);
        }

        return result;
    }

    @Override
    public List<String> listValidVirtualIds(int storageIndex) {
        return getVirtualIdListByStorageId(storageIndex, STATE_VALID);
    }

    @Override
    public List<String> listVirtualIds(int storageId) {
        List<String> result = new ArrayList<String>();
        try {
            String path = ZKPaths.makePath(virtualIdContainer, String.valueOf(storageId));
            List<String> nodeList = client.getChildren().forPath(path);
            if (nodeList != null) {
                for (String node : nodeList) {
                    String nodePath = ZKPaths.makePath(virtualIdContainer, String.valueOf(storageId), node);
                    List<String> childs = client.getChildren().forPath(nodePath);
                    if (childs != null && !childs.isEmpty()) {
                        result.add(node);
                    }
                }
            }
        } catch (NoNodeException e) {
            //ignore
        } catch (Exception e) {
            LOG.error("get virtual id by sn[{}] node error", storageId, e);
        }

        return result;
    }

    @Override
    public boolean hasVirtual(int storageIndex, String virtualId, String first) {
        String virtualIdNodePath = ZKPaths.makePath(virtualIdContainer, String.valueOf(storageIndex), virtualId, first);
        try {
            if (client.checkExists().forPath(virtualIdNodePath) == null) {
                return false;
            }
            byte[] data = client.getData().forPath(virtualIdNodePath);
            if (data == null) {
                return false;
            }
            return (Ints.fromByteArray(data) & STATE_VALID) > 0;
        } catch (Exception e) {
            LOG.error("judage valid virtual id happen error {},", virtualIdNodePath, e);
        }
        return true;
    }

    @Override
    public List<String> listInvalidVirtualIds(int storageIndex) {
        return getVirtualIdListByStorageId(storageIndex, STATE_INVALID);
    }

    @Override
    public void addFirstId(int storageIndex, String virtualId, String firstId) {
        LOG.info("register first id :" + firstId);
        String virtualIdNodePath = ZKPaths.makePath(virtualIdContainer, String.valueOf(storageIndex), virtualId);

        Stat nodeStat = null;
        try {
            nodeStat = client.checkExists().forPath(virtualIdNodePath);
        } catch (Exception e) {
            LOG.error("check virtual id[{}:{}] vadility error", storageIndex, virtualId, e);
        }

        if (nodeStat == null) {
            return;
        }

        try {
            client.create().withMode(CreateMode.PERSISTENT).forPath(ZKPaths.makePath(virtualIdNodePath, firstId));
        } catch (Exception e) {
            LOG.error("add first id error", e);
        }
    }

}
