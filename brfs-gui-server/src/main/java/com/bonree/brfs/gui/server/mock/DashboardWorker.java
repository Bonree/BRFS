package com.bonree.brfs.gui.server.mock;

import com.bonree.brfs.client.BRFS;
import com.bonree.brfs.client.BRFSClientBuilder;
import com.bonree.brfs.client.discovery.Discovery;
import com.bonree.brfs.client.discovery.ServerNode;
import com.bonree.brfs.common.ZookeeperPaths;
import com.bonree.brfs.common.resource.vo.DataNodeMetaModel;
import com.bonree.brfs.common.resource.vo.DiskPartitionStat;
import com.bonree.brfs.common.resource.vo.NodeSnapshotInfo;
import com.bonree.brfs.common.utils.JsonUtils;
import com.bonree.brfs.gui.server.AlertConfig;
import com.bonree.brfs.gui.server.BrfsConfig;
import com.bonree.brfs.gui.server.GuiInnerClient;
import com.bonree.brfs.gui.server.TotalDiskUsage;
import com.bonree.brfs.gui.server.node.NodeState;
import com.bonree.brfs.gui.server.node.NodeSummaryInfo;
import com.bonree.brfs.gui.server.node.ServerState;
import com.bonree.brfs.gui.server.zookeeper.ZookeeperConfig;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.inject.Inject;
import okhttp3.HttpUrl;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import okhttp3.ResponseBody;
import org.apache.commons.lang3.StringUtils;
import org.apache.curator.ensemble.fixed.FixedEnsembleProvider;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.imps.DefaultACLProvider;
import org.apache.curator.retry.BoundedExponentialBackoffRetry;
import org.apache.curator.utils.ZKPaths;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DashboardWorker {
    private static final int BASE_SLEEP_TIME_MS = 1000;

    private static final int MAX_SLEEP_TIME_MS = 45000;

    private static final int MAX_RETRIES = 30;
    private static final Logger LOG = LoggerFactory.getLogger(DashboardWorker.class);
    private static final ThreadFactory FACTORY = new ThreadFactoryBuilder()
        .setDaemon(true)
        .setNameFormat("DashboardWorker")
        .build();
    private BrfsConfig config;
    private AlertConfig alertLine;
    private GuiInnerClient innerClient;
    private OkHttpClient client;
    private ExecutorService pool = Executors.newFixedThreadPool(3, FACTORY);
    private ZookeeperConfig zkConfig;
    private CuratorFramework zkClient;

    private ZookeeperPaths zkPaths;

    @Inject
    public DashboardWorker(BrfsConfig config, AlertConfig alertLine, ZookeeperConfig zkConfig, GuiInnerClient innerClient,
                           OkHttpClient client) {
        this.config = config;
        this.alertLine = alertLine;
        this.zkConfig = zkConfig;
        this.innerClient = innerClient;
        this.client = client;
        this.zkClient = CuratorFrameworkFactory.builder()
                                               .ensembleProvider(new FixedEnsembleProvider(zkConfig.getAddresses()))
                                               .sessionTimeoutMs(10000)
                                               .retryPolicy(new BoundedExponentialBackoffRetry(BASE_SLEEP_TIME_MS,
                                                                                               MAX_SLEEP_TIME_MS,
                                                                                               MAX_RETRIES))
                                               .aclProvider(new DefaultACLProvider())
                                               .build();
        ;
    }

    @PostConstruct
    public void start() throws Exception {
        this.zkClient.start();
        this.zkClient.blockUntilConnected();
        zkPaths = ZookeeperPaths.getBasePath(config.getClusterName(), this.zkClient);
        LOG.info("DashboardWorker start");
    }

    @PreDestroy
    public void stop() throws Exception {
        LOG.info("DashboardWorker stop");
        this.zkClient.close();
    }

    public List<String> getBusinesses() {
        try {
            BRFS client = new BRFSClientBuilder().build(innerClient.geturis());
            List<String> storageRegions = client.listStorageRegions();
            Set<String> set = new HashSet<>();
            for (String storageRegion : storageRegions) {
                String key = getBusinessByRlue(storageRegion);
                set.add(key);
            }
            return ImmutableList.copyOf(set);
        } catch (Exception e) {
            LOG.error("get storage happen error", e);
        }
        return ImmutableList.of();
    }

    private String getBusinessByRlue(String region) {
        String[] array = StringUtils.split(region, "_");
        if (array == null || array.length < 2) {
            return region;
        }
        return array[0] + "_" + array[1];
    }

    public TotalDiskUsage getTotalDiskUsage() {
        try {
            List<NodeSnapshotInfo> snapshotInfos = collectResource();
            if (snapshotInfos == null || snapshotInfos.isEmpty()) {
                return new TotalDiskUsage(0, 0);
            }
            long free = 0;
            long useds = 0;
            for (NodeSnapshotInfo node : snapshotInfos) {
                Collection<DiskPartitionStat> stats = node.getDiskPartitionStats();
                long tmpfree = stats.stream()
                                    .mapToLong(DiskPartitionStat::getFree)
                                    .sum();
                long tmpUsed = stats.stream()
                                    .mapToLong(DiskPartitionStat::getUsed)
                                    .sum();
                free += tmpfree;
                useds += tmpUsed;

            }
            return new TotalDiskUsage(useds, free);
        } catch (Exception e) {
            LOG.error("collect total disk usage happen error ", e);
        }
        return new TotalDiskUsage(0, 0);
    }

    public List<NodeSummaryInfo> getNodeSummaries() {
        try {
            List<NodeSnapshotInfo> snapshotInfos = collectResource();
            List<ServerNode> regions = innerClient.getServiceList(Discovery.ServiceType.REGION);
            Map<String, ServerNode> regionMap = new HashMap<>();
            Map<String, NodeSnapshotInfo> snapshotMap = new HashMap<>();
            Map<String, DataNodeMetaModel> metaMap = collectDataMetaNodes();
            for (NodeSnapshotInfo node : snapshotInfos) {
                String host = node.getHost();
                if (host.contains(":")) {
                    host = host.substring(0, host.indexOf(":"));
                }
                snapshotMap.put(host, node);
            }
            for (ServerNode server : regions) {
                regionMap.put(server.getHost(), server);
            }
            Set<String> keys = new HashSet<String>();
            keys.addAll(regionMap.keySet());
            keys.addAll(snapshotMap.keySet());
            keys.addAll(metaMap.keySet());
            List<NodeSummaryInfo> summaryInfos = new ArrayList<>();
            for (String key : keys) {
                NodeSummaryInfo summary = packageSummaryInfo(regionMap.get(key), snapshotMap.get(key), metaMap.get(key));
                summaryInfos.add(summary);
            }
            return summaryInfos;
        } catch (Exception e) {
            LOG.error("get node summaries happen error ", e);
        }
        return ImmutableList.of();
    }

    public NodeSummaryInfo packageSummaryInfo(ServerNode region, NodeSnapshotInfo data, DataNodeMetaModel model) {
        if (region == null && data == null) {
            if (model == null) {
                return null;
            }
            return new NodeSummaryInfo(
                ServerState.DEAD,
                NodeState.OFFLINE,
                NodeState.OFFLINE,
                model.getIp(),
                model.getIp(),
                0.0,
                0.0,
                0.0,
                0.0);
        } else if (data == null) {
            return new NodeSummaryInfo(
                ServerState.HEALTH,
                NodeState.ONLINE,
                NodeState.OFFLINE,
                region.getHost(),
                region.getHost(),
                0.0,
                0.0,
                0.0,
                0.0);
        } else if (region == null) {
            Collection<DiskPartitionStat> stats = data.getDiskPartitionStats();
            long total = stats.stream().mapToLong(DiskPartitionStat::getTotal).sum();
            long usage = stats.stream().mapToLong(DiskPartitionStat::getUsed).sum();
            double cpuRate = data.getCpustat().getTotal() * 100;
            double memRate = data.getMemStat().getUsedPercent();
            double diskRate = ((double) usage) / total * 100;
            ServerState state = getServerState(cpuRate, memRate, diskRate);
            return new NodeSummaryInfo(state,
                                       NodeState.OFFLINE,
                                       NodeState.ONLINE,
                                       data.getHost(),
                                       data.getHost(),
                                       cpuRate * 100,
                                       memRate,
                                       diskRate,
                                       0.0);
        } else {
            Collection<DiskPartitionStat> stats = data.getDiskPartitionStats();
            long total = stats.stream().mapToLong(DiskPartitionStat::getTotal).sum();
            long usage = stats.stream().mapToLong(DiskPartitionStat::getUsed).sum();
            double cpuRate = data.getCpustat().getTotal();
            double memRate = data.getMemStat().getUsedPercent();
            double diskRate = ((double) usage) / total;
            ServerState state = getServerState(cpuRate, memRate, diskRate);
            return new NodeSummaryInfo(state,
                                       NodeState.ONLINE,
                                       NodeState.ONLINE,
                                       data.getHost(),
                                       data.getHost(),
                                       cpuRate * 100,
                                       memRate,
                                       diskRate,
                                       0.0);
        }
    }

    private ServerState getServerState(double cpuRate, double memRate, double dataDiskRate) {
        if (cpuRate > alertLine.getAlertLineCpuPercent()
            || memRate > alertLine.getAlertLineMemPercent()
            || dataDiskRate > alertLine.getAlertLineDataDiskPercent()) {
            return ServerState.ALERT;
        }
        return ServerState.HEALTH;
    }

    public Map<String, DataNodeMetaModel> collectDataMetaNodes() {
        try {
            String basePath = zkPaths.getBaseDataNodeMetaPath();
            if (zkClient.checkExists().forPath(basePath) == null) {
                return ImmutableMap.of();
            }
            List<String> childs = zkClient.getChildren().forPath(basePath);
            if (childs == null || childs.isEmpty()) {
                return ImmutableMap.of();
            }
            Map<String, DataNodeMetaModel> map = new HashMap<>();
            for (String child : childs) {
                String childPath = ZKPaths.makePath(basePath, child);
                byte[] data = zkClient.getData().forPath(childPath);
                if (data == null || data.length == 0) {
                    continue;
                }
                DataNodeMetaModel model = JsonUtils.toObjectQuietly(data, DataNodeMetaModel.class);
                if (model != null) {
                    map.put(model.getIp(), model);
                }
            }
            return map;
        } catch (Exception e) {
            LOG.error("load dataMetaNodes happen error ", e);
        }
        return ImmutableMap.of();
    }

    public List<NodeSnapshotInfo> collectResource() throws Exception {
        List<ServerNode> services = innerClient.getServiceList(Discovery.ServiceType.DATA);
        if (services == null || services.isEmpty()) {
            return ImmutableList.of();
        }
        List<ResourceCall> requests = new ArrayList<>();
        services.stream().forEach(server -> {
            HttpUrl url = new HttpUrl.Builder()
                .host(server.getHost())
                .port(config.getDataNodePort())
                .encodedPath("/resource")
                .scheme("http")
                .build();
            Request request = new Request.Builder().url(url).build();
            ResourceCall call = new ResourceCall(request, client);
            requests.add(call);
        });
        if (requests.isEmpty()) {
            return ImmutableList.of();
        }
        List<Future<NodeSnapshotInfo>> futures = new ArrayList<>();
        for (ResourceCall call : requests) {
            futures.add(this.pool.submit(call));
        }
        List<NodeSnapshotInfo> snapshotInfos = new ArrayList<>();
        futures.stream().forEach(x -> {
            try {
                NodeSnapshotInfo node = x.get();
                if (node != null) {
                    snapshotInfos.add(node);
                }
            } catch (Exception e) {
                LOG.error("happen error ", e);
            }
        });
        return snapshotInfos;
    }

    private class ResourceCall implements Callable<NodeSnapshotInfo> {
        private Request request;
        private OkHttpClient client;

        public ResourceCall(Request request, OkHttpClient client) {
            this.request = request;
            this.client = client;
        }

        @Override
        public NodeSnapshotInfo call() throws Exception {
            NodeSnapshotInfo snapshotInfo = null;
            Response response = null;
            int retryCount = 3;
            while (retryCount > 0) {
                try {
                    response = client.newCall(request).execute();
                    if (response.code() != 200) {
                        LOG.warn("request response code not ok {}", response.message());
                        continue;
                    }
                    ResponseBody body = response.body();
                    if (body == null) {
                        LOG.warn("request response body is null");
                        continue;
                    }
                    byte[] data = body.bytes();
                    if (data == null || data.length == 0) {
                        LOG.warn("request response body data is null");
                        continue;
                    }
                    snapshotInfo = JsonUtils.toObjectQuietly(data, NodeSnapshotInfo.class);
                    if (snapshotInfo == null) {
                        LOG.warn("convert to object is null {}", data == null ? "" : new String(data));
                        continue;
                    }
                } catch (IOException e) {
                    LOG.error("request node happen error ", e);
                } finally {
                    retryCount--;
                    try {
                        Thread.sleep(300);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
            return snapshotInfo;
        }
    }
}
