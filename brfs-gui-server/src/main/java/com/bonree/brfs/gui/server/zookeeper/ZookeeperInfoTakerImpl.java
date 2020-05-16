package com.bonree.brfs.gui.server.zookeeper;

import com.google.common.collect.ImmutableList;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.inject.Inject;
import org.apache.curator.ensemble.fixed.FixedEnsembleProvider;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.imps.DefaultACLProvider;
import org.apache.curator.retry.BoundedExponentialBackoffRetry;
import org.apache.curator.utils.ZKPaths;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZookeeperInfoTakerImpl implements ZookeeperInfoTaker {
    private static final Logger log = LoggerFactory.getLogger(ZookeeperInfoTakerImpl.class);

    private static final int BASE_SLEEP_TIME_MS = 1000;

    private static final int MAX_SLEEP_TIME_MS = 45000;

    private static final int MAX_RETRIES = 30;

    private final CuratorFramework client;
    private final String rootNode;

    @Inject
    public ZookeeperInfoTakerImpl(ZookeeperConfig config) {
        this.client = CuratorFrameworkFactory.builder()
            .ensembleProvider(new FixedEnsembleProvider(config.getAddresses()))
            .sessionTimeoutMs(10000)
            .retryPolicy(new BoundedExponentialBackoffRetry(BASE_SLEEP_TIME_MS, MAX_SLEEP_TIME_MS, MAX_RETRIES))
            .aclProvider(new DefaultACLProvider())
            .build();

        this.rootNode = config.getRoot();
    }

    @PostConstruct
    public void start() throws InterruptedException {
        log.info("start zookeeper client");
        client.start();
        client.blockUntilConnected();
    }

    @PreDestroy
    public void stop() {
        log.info("stop zookeeper client");
        client.close();
    }

    @Override
    public ZookeeperNode rootNode() {
        return getNode(rootNode);
    }

    @Override
    public List<ZookeeperNode> list(String nodePath) {
        try {
            List<String> children = client.getChildren().forPath(nodePath);
            return children.stream()
                .map(child -> ZKPaths.makePath(nodePath, child))
                .map(this::getNode)
                .collect(Collectors.toList());
        } catch (Exception e) {
            log.error("list children for {} error", nodePath, e);
        }

        return ImmutableList.of();
    }

    @Override
    public ZookeeperNodeData getData(String nodePath) {
        String data = "";
        try {
            byte[] bytes = client.getData().forPath(nodePath);
            if (bytes.length > 0) {
                data = new String(bytes, StandardCharsets.UTF_8);
            }
        } catch (Exception e) {
            log.error("get data for {} error", nodePath, e);
        }

        return new ZookeeperNodeData(data);
    }

    public ZookeeperNode getNode(String path) {
        try {
            byte[] bytes = client.getData().forPath(path);
            return new ZookeeperNode(ZKPaths.getNodeFromPath(path), bytes.length > 0);
        } catch (Exception e) {
            log.error("get data for {} error", path, e);
        }

        return new ZookeeperNode(ZKPaths.getNodeFromPath(path), false);
    }
}
