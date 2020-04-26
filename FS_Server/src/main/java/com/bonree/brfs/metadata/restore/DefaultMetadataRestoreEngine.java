package com.bonree.brfs.metadata.restore;

import com.bonree.brfs.metadata.ZNode;
import java.util.List;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Transaction;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*******************************************************************************
 * 版权信息：北京博睿宏远数据科技股份有限公司
 * Copyright: Copyright (c) 2007博睿宏远科技发展有限公司,Inc.All Rights Reserved.
 *
 * @Date 2020/4/17 14:34
 * @Author: <a href=mailto:zhangqi@bonree.com>张奇</a>
 * @Description: 默认元数据恢复引擎
 ******************************************************************************/
public class DefaultMetadataRestoreEngine implements MetadataRestoreEngine {

    private static final Logger LOG = LoggerFactory.getLogger(DefaultMetadataRestoreEngine.class);

    private ZNode sourceRoot;
    private String destPath;
    private ZooKeeper zk;
    private boolean ignoreEphemeralNodes;
    private boolean removeDeprecated;
    private long ephemeralIgnored = 0;
    private long deletedEphemeral = 0;
    private long nodesUpdated = 0;
    private long nodesCreated = 0;
    private long nodesSkipped = 0;
    private long mtime;
    private long maxMtime;
    private Transaction transaction;
    private int batchSize;

    /**
     * Create new {@link DefaultMetadataRestoreEngine} instance.
     *
     * @param zk                    Zookeeper server
     * @param znode                 root node to copy data from
     * @param removeDeprecatedNodes {@code true} if nodes that does not exist in source should be
     *                              removed
     * @param ignoreEphemeralNodes  {@code true} if ephemeral nodes should not be copied
     * @param mtime                 znodes modified before this timestamp will not be copied.
     */
    public DefaultMetadataRestoreEngine(ZooKeeper zk, String destPath, ZNode znode, boolean removeDeprecatedNodes,
                                        boolean ignoreEphemeralNodes, long mtime, int batchSize) {
        this.zk = zk;
        this.destPath = destPath;
        this.sourceRoot = znode;
        this.removeDeprecated = removeDeprecatedNodes;
        this.ignoreEphemeralNodes = ignoreEphemeralNodes;
        this.mtime = mtime;
        this.batchSize = batchSize;
    }

    /**
     * Start process of writing data to the target.
     */
    @Override
    public void restore() {
        try {
            ZNode dest = sourceRoot;
            dest.setPath(destPath);
            LOG.info("Writing data to [{}].", destPath);
            transaction = new AutoCommitTransactionWrapper(zk, batchSize);
            update(dest);
            transaction.commit();
            LOG.info("Writing data to [{}] completed.", destPath);
            LOG.info("Total Write [{}] nodes", nodesCreated + nodesUpdated);
            LOG.info("Created [{}] nodes; Updated [{}] nodes", nodesCreated, nodesUpdated);
            LOG.info("Ignored [{}] ephemeral nodes", ephemeralIgnored);
            LOG.info("Skipped [{}] nodes older than [{}]", nodesSkipped, mtime);
            LOG.info("Max modify time of copied nodes: [{}]", maxMtime);
            if (deletedEphemeral > 0) {
                LOG.info("Deleted [{}] ephemeral nodes", deletedEphemeral);
            }

        } catch (KeeperException | InterruptedException e) {
            LOG.error("Exception caught while writing nodes", e);
        }
    }

    private void update(ZNode node) throws KeeperException, InterruptedException {
        String path = node.getAbsolutePath();
        if (ignoreEphemeralNodes && node.isEphemeral()) {
            ephemeralIgnored++;
            Stat stat = zk.exists(path, false);
            // 1. only delete ephemeral nodes if they've been copied over persistently before
            if (stat != null && stat.getEphemeralOwner() == 0) {
                transaction.delete(path, stat.getVersion());
                deletedEphemeral++;
            }
            return;
        }

        if (node.getMtime() > mtime) {
            upsertNode(node);
            maxMtime = Math.max(node.getMtime(), maxMtime);
        } else {
            nodesSkipped++;
        }

        // 2. Recursively update or create children
        for (ZNode child : node.getChildren()) {
            update(child);
        }

        if (removeDeprecated) {
            // 3. Remove deprecated children
            try {
                List<String> destChildren = zk.getChildren(path, false);
                for (String child : destChildren) {
                    if (!node.getChildrenNames().contains(child)) {
                        delete(node.getAbsolutePath() + "/" + child);
                    }
                }
            } catch (KeeperException e) {
                if (e.code() == KeeperException.Code.NONODE) {
                    // If there was no such node before this transaction started, then it can't have
                    // any children and is therefore safe to ignore
                    return;
                }
                throw e;
            }
        }
    }

    /**
     * Updates or creates the given node.
     *
     * @param node The node to copy
     *
     * @throws KeeperException      If the server signals an error
     * @throws InterruptedException If the server transaction is interrupted
     */
    private void upsertNode(ZNode node) throws KeeperException, InterruptedException {
        String nodePath = node.getAbsolutePath();
        //  Update or create current node
        Stat stat = zk.exists(nodePath, false);
        if (stat != null) {
            LOG.debug("Attempting to update [{}]", nodePath);
            transaction.setData(nodePath, node.getData(), -1);
            nodesUpdated++;
        } else {
            LOG.debug("Attempting to create [{}]", nodePath);
            transaction.create(nodePath, node.getData(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            nodesCreated++;
        }
        if (nodesUpdated % 100 == 0) {
            LOG.debug("Updated: [{}], current node mtime [{}]", nodesUpdated, node.getMtime());
        }
    }

    private void delete(String path) throws KeeperException, InterruptedException {
        List<String> children = zk.getChildren(path, false);
        for (String child : children) {
            delete(path + "/" + child);
        }
        transaction.delete(path, -1);
        LOG.info("Deleted node [{}]", path);
    }
}
