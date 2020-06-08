package com.bonree.brfs.gui.server.resource.maintain;

import com.bonree.brfs.client.discovery.Discovery;
import com.bonree.brfs.client.discovery.ServerNode;
import com.bonree.brfs.common.process.LifeCycle;
import com.bonree.brfs.common.resource.vo.NodeSnapshotInfo;
import com.bonree.brfs.common.utils.JsonUtils;
import com.bonree.brfs.common.utils.TimeUtils;
import com.bonree.brfs.gui.server.BrfsConfig;
import com.bonree.brfs.gui.server.GuiInnerClient;
import com.bonree.brfs.gui.server.GuiResourceConfig;
import com.bonree.brfs.gui.server.resource.GuiResourceMaintainer;
import com.bonree.brfs.gui.server.resource.ResourceHandlerInterface;
import com.bonree.brfs.gui.server.resource.impl.GuiFileMaintainer;
import com.bonree.brfs.gui.server.resource.impl.ResourceHandler;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.inject.Inject;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import okhttp3.HttpUrl;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import okhttp3.ResponseBody;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ResourceRequestMaintainer implements LifeCycle {
    private static final Logger LOG = LoggerFactory.getLogger(ResourceRequestMaintainer.class);
    private GuiInnerClient httpDiscovery;
    private ResourceHandlerInterface convertor;
    private GuiResourceMaintainer guiFileMaintainer;
    private int intervalTime;
    private ScheduledExecutorService pool;
    private List<ScheduledFuture<?>> futures = new ArrayList<>();
    private int port;
    private BlockingQueue<NodeSnapshotInfo> queue = new LinkedBlockingQueue<>(100);
    private OkHttpClient client;

    @Inject
    public ResourceRequestMaintainer(
        OkHttpClient client, GuiInnerClient httpDiscovery, ResourceHandlerInterface convertor,
        GuiResourceMaintainer guiFileMaintainer, GuiResourceConfig config, BrfsConfig brfsConfig) {
        this.client = client;
        this.httpDiscovery = httpDiscovery;
        this.convertor = convertor;
        this.guiFileMaintainer = guiFileMaintainer;
        this.intervalTime = config.getIntervalTime();
        this.port = brfsConfig.getDataNodePort();
    }

    private class RequestWorker implements Runnable {
        @Override
        public void run() {

            List<ServerNode> serverNodes = null;
            try {
                // 1.get server list
                serverNodes = httpDiscovery.getServiceList(Discovery.ServiceType.DATA);
                if (serverNodes == null || serverNodes.isEmpty()) {
                    LOG.warn("the cluster server is empty !!");
                    return;
                }
            } catch (Exception e) {
                LOG.error("happen when request servers", e);
                return;
            }
            // 2. create request queue
            Collection<Request> requests = new HashSet<>();
            serverNodes.stream().forEach(
                x -> {
                    HttpUrl url = new HttpUrl.Builder()
                        .host(x.getHost())
                        .port(port)
                        .encodedPath("/resource")
                        .scheme("http")
                        .build();
                    Request request = new Request.Builder().url(url).build();
                    requests.add(request);
                }
            );
            // 3. send request
            if (requests == null || requests.isEmpty()) {
                LOG.warn("the request queue is empty !!");
                return;
            }
            requests.stream().parallel().forEach(x -> {
                Response response = null;
                int retryCount = 3;
                while (retryCount > 0) {
                    try {
                        response = client.newCall(x).execute();
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
                        NodeSnapshotInfo snapshotInfo = JsonUtils.toObjectQuietly(data, NodeSnapshotInfo.class);
                        if (snapshotInfo == null) {
                            LOG.warn("convert to object is null {}", data == null ? "" : new String(data));
                            continue;
                        }
                        queue.add(snapshotInfo);
                        break;
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

            });

        }
    }

    private class ConvertWorker implements Runnable {
        @Override
        public void run() {
            NodeSnapshotInfo snapshotInfo;

            while (true) {
                try {
                    snapshotInfo = queue.take();
                    String id = snapshotInfo.getNodeId();
                    LOG.info("get node data {} {}", id, TimeUtils.formatTimeStamp(snapshotInfo.getTime(), "yyyy-MM-dd HH:mm:ss"));
                    guiFileMaintainer.setNodeInfo(id, convertor.gatherNodeInfo(snapshotInfo));
                    guiFileMaintainer.setCpuInfo(id, convertor.gatherCpuInfo(snapshotInfo));
                    guiFileMaintainer.setLoadInfo(id, convertor.gatherLoadInfo(snapshotInfo));
                    guiFileMaintainer.setMemInfo(id, convertor.gahterMemInfo(snapshotInfo));
                    guiFileMaintainer.setDiskIOs(id, convertor.gatherDiskIOInfos(snapshotInfo));
                    guiFileMaintainer.setDiskUsages(id, convertor.gatherDiskUsageInfos(snapshotInfo));
                    guiFileMaintainer.setNetInfos(id, convertor.gatherNetInfos(snapshotInfo));
                } catch (Exception e) {
                    LOG.error("convert resource happen error ", e);
                }
            }

        }
    }

    @Override
    public void start() throws Exception {
        LOG.info("resource request maintainer start");

        // 2.初始化线程池
        ThreadFactory threadFactory = new ThreadFactoryBuilder()
            .setDaemon(true)
            .setNameFormat("ResourceRequestMaintainer")
            .build();
        this.pool = Executors.newScheduledThreadPool(2, threadFactory);

        ScheduledFuture<?> future = this.pool.scheduleAtFixedRate(new RequestWorker(), 0, intervalTime, TimeUnit.SECONDS);
        ScheduledFuture<?> future1 = pool.schedule(new ConvertWorker(), 0, TimeUnit.SECONDS);

        futures.add(future);
        futures.add(future1);
    }

    @Override
    public void stop() throws Exception {
        LOG.info("resource request maintainer stop");
        futures.stream().forEach(x -> {
            x.cancel(true);
        });
        if (this.pool != null) {
            this.pool.shutdownNow();
        }
    }

    public static void main(String[] args) throws Exception {
        OkHttpClient client = new OkHttpClient.Builder()
            .callTimeout(Duration.ofSeconds(10))
            .connectTimeout(Duration.ofSeconds(10))
            .readTimeout(Duration.ofSeconds(10))
            .writeTimeout(Duration.ofSeconds(10))
            .build();
        BrfsConfig brfsConfig = new BrfsConfig();
        brfsConfig.setDataNodePort(9999);
        brfsConfig.setPassword("123456");
        brfsConfig.setUsername("root");
        brfsConfig.setRegionAddress(Arrays.asList(
            "http://192.168.150.237:9200"
        ));

        GuiInnerClient httpDiscovery = new GuiInnerClient(client, brfsConfig);
        // 2. create request queue
        ResourceHandlerInterface convertor = new ResourceHandler();

        GuiResourceConfig config = new GuiResourceConfig();
        config.setGuiDir("/data/br/brfs/gui");
        config.setIntervalTime(60);
        config.setScanIntervalTime(600);
        config.setTtlTime(7 * 24 * 60 * 60);
        GuiFileMaintainer guiFileMaintainer = new GuiFileMaintainer(config);
        guiFileMaintainer.start();
        ResourceRequestMaintainer maintainer = new ResourceRequestMaintainer(
            client, httpDiscovery, convertor, guiFileMaintainer, config, brfsConfig);
        maintainer.start();
        Thread.sleep(Integer.MAX_VALUE);
    }
}
