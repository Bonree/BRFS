package com.bonree.brfs.metadata.restore;

import java.util.List;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.OpResult;
import org.apache.zookeeper.Transaction;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*******************************************************************************
 * 版权信息：北京博睿宏远数据科技股份有限公司
 * Copyright: Copyright (c) 2007博睿宏远科技发展有限公司,Inc.All Rights Reserved.
 *
 * @Date 2020/4/16 15:24
 * @Author: <a href=mailto:zhangqi@bonree.com>张奇</a>
 * @Description:
 ******************************************************************************/
public class AutoCommitTransactionWrapper extends Transaction {

    private static final Logger LOG = LoggerFactory.getLogger(AutoCommitTransactionWrapper.class);

    private Transaction transaction;
    private int transactionSize;
    private int opsSinceCommit = 0;
    private ZooKeeper zk;

    /**
     * @param zk              Zookeeper server to commit transactions to.
     * @param transactionSize Number of operations to perform before commit, <em>n.b you will
     *                        have to perform you last {@link #commit()} manually </em>
     */
    protected AutoCommitTransactionWrapper(ZooKeeper zk, int transactionSize) {
        super(zk);
        this.transaction = zk.transaction();
        this.zk = zk;
        this.transactionSize = transactionSize;
    }

    @Override
    public Transaction create(String path, byte[] data, List<ACL> acl, CreateMode createMode) {
        maybeCommitTransaction();
        return transaction.create(path, data, acl, createMode);
    }

    @Override
    public Transaction delete(String path, int version) {
        maybeCommitTransaction();
        return transaction.delete(path, version);
    }

    @Override
    public Transaction check(String path, int version) {
        maybeCommitTransaction();
        return transaction.check(path, version);
    }

    @Override
    public Transaction setData(String path, byte[] data, int version) {
        maybeCommitTransaction();
        return transaction.setData(path, data, version);
    }

    @Override
    public List<OpResult> commit() throws InterruptedException, KeeperException {
        return transaction.commit();
    }

    private void maybeCommitTransaction() {
        if (opsSinceCommit >= (transactionSize - 1)) {
            try {
                LOG.info("Committing transaction");
                transaction.commit();
                opsSinceCommit = 0;
                transaction = zk.transaction();
            } catch (InterruptedException | KeeperException e) {
                throw new RuntimeException(e);
            }
        } else {
            opsSinceCommit++;
        }

    }
}
