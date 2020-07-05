package com.bonree.brfs.gui.server.mock;

import static com.bonree.brfs.client.utils.Strings.format;

import com.bonree.brfs.client.BRFS;
import com.bonree.brfs.client.BRFSClient;
import com.bonree.brfs.client.BRFSClientBuilder;
import com.bonree.brfs.client.discovery.Discovery;
import com.bonree.brfs.client.discovery.ServerNode;
import com.bonree.brfs.client.utils.HttpStatus;
import com.bonree.brfs.client.utils.URIRetryable;
import com.bonree.brfs.common.resource.vo.DiskPartitionStat;
import com.bonree.brfs.common.resource.vo.NodeSnapshotInfo;
import com.bonree.brfs.common.utils.JsonUtils;
import com.bonree.brfs.gui.server.BrfsConfig;
import com.bonree.brfs.gui.server.GuiInnerClient;
import com.bonree.brfs.gui.server.TotalDiskUsage;
import com.bonree.brfs.gui.server.node.NodeState;
import com.bonree.brfs.gui.server.node.NodeSummaryInfo;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.io.IOException;
import java.net.URI;
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
import javax.inject.Inject;
import okhttp3.HttpUrl;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import okhttp3.ResponseBody;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.net.util.IPAddressUtil;

public class DashboardWorker {
    private static final Logger LOG = LoggerFactory.getLogger(DashboardWorker.class);
    private static final ThreadFactory FACTORY = new ThreadFactoryBuilder()
        .setDaemon(true)
        .setNameFormat("DashboardWorker")
        .build();
    private BrfsConfig config;
    private GuiInnerClient innerClient;
    private OkHttpClient client;
    private ExecutorService pool = Executors.newFixedThreadPool(3, FACTORY);

    @Inject
    public DashboardWorker(BrfsConfig config, GuiInnerClient innerClient, OkHttpClient client) {
        this.config = config;
        this.innerClient = innerClient;
        this.client = client;
    }

    public List<String> getBusinesses() {
        try {
            BRFS client = new BRFSClientBuilder().build(innerClient.geturis());
            return client.listStorageRegions();
        } catch (Exception e) {
            LOG.error("get storage happen error", e);
        }
        return ImmutableList.of();
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
            List<NodeSummaryInfo> summaryInfos = new ArrayList<>();
            for (String key : keys) {
                NodeSummaryInfo summary = packageSummaryInfo(regionMap.get(key), snapshotMap.get(key));
                summaryInfos.add(summary);
            }
            return summaryInfos;
        } catch (Exception e) {
            LOG.error("get node summaries happen error ", e);
        }
        return ImmutableList.of();
    }

    public NodeSummaryInfo packageSummaryInfo(ServerNode region, NodeSnapshotInfo data) {
        if (region == null && data == null) {
            return null;
        } else if (data == null) {
            return new NodeSummaryInfo(NodeState.ONLINE,
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
            return new NodeSummaryInfo(NodeState.ONLINE,
                                       data.getHost(),
                                       data.getHost(),
                                       data.getCpustat().getTotal() * 100,
                                       data.getMemStat().getUsedPercent(),
                                       ((double) usage) / total,
                                       0.0);
        } else {
            Collection<DiskPartitionStat> stats = data.getDiskPartitionStats();
            long total = stats.stream().mapToLong(DiskPartitionStat::getTotal).sum();
            long usage = stats.stream().mapToLong(DiskPartitionStat::getUsed).sum();
            return new NodeSummaryInfo(NodeState.ONLINE,
                                       data.getHost(),
                                       data.getHost(),
                                       data.getCpustat().getTotal() * 100,
                                       data.getMemStat().getUsedPercent(),
                                       ((double) usage) / total,
                                       0.0);
        }
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
