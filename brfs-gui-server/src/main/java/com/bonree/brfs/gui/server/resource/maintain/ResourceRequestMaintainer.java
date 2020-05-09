package com.bonree.brfs.gui.server.resource.maintain;

import com.bonree.brfs.client.BRFSClientBuilder;
import com.bonree.brfs.client.discovery.Discovery;
import com.bonree.brfs.client.discovery.HttpDiscovery;
import com.bonree.brfs.client.discovery.ServerNode;
import com.bonree.brfs.client.utils.Retryable;
import com.bonree.brfs.client.utils.Retrys;
import com.bonree.brfs.client.utils.SocketChannelSocketFactory;
import com.bonree.brfs.common.process.LifeCycle;
import com.bonree.brfs.common.resource.vo.NodeSnapshotInfo;
import com.bonree.brfs.common.utils.JsonUtils;
import com.bonree.brfs.gui.server.resource.ResourceHandlerInterface;
import com.bonree.brfs.gui.server.resource.impl.GuiFileMaintainer;
import com.google.common.io.Closer;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Queue;
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
    private HttpDiscovery httpDiscovery;
    private ResourceHandlerInterface convertor;
    private GuiFileMaintainer guiFileMaintainer;
    private int intervalTime;
    private OkHttpClient client;
    private Closer closer;
    private ScheduledExecutorService pool;
    private List<ScheduledFuture<?>> futures = new ArrayList<>();
    private boolean runFlag = false;
    private BlockingQueue<NodeSnapshotInfo> queue = new LinkedBlockingQueue<>(3);

    public ResourceRequestMaintainer(HttpDiscovery httpDiscovery, ResourceHandlerInterface convertor,
                                     GuiFileMaintainer guiFileMaintainer, int intervalTime) {
        this.httpDiscovery = httpDiscovery;
        this.convertor = convertor;
        this.guiFileMaintainer = guiFileMaintainer;
        this.intervalTime = intervalTime;

    }

    @Override
    public void start() throws Exception {
        // 1.初始化http请求
        closer = Closer.create();

        OkHttpClient httpClient = new OkHttpClient.Builder()
            .socketFactory(new SocketChannelSocketFactory())
            .callTimeout(Duration.ofSeconds(10))
            .connectTimeout(Duration.ofSeconds(10))
            .readTimeout(Duration.ofSeconds(10))
            .writeTimeout(Duration.ofSeconds(10))
            .build();
        closer.register(() -> {
            httpClient.dispatcher().executorService().shutdown();
            httpClient.connectionPool().evictAll();
        });
        // 2.初始化线程池
        ThreadFactory threadFactory = new ThreadFactoryBuilder()
            .setDaemon(true)
            .setNameFormat("ResourceRequestMaintainer")
            .build();
        this.pool = Executors.newScheduledThreadPool(2, threadFactory);

        ScheduledFuture<?> future = this.pool.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                runFlag = true;
                // 1.get server list
                List<ServerNode> serverNodes = httpDiscovery.getServiceList(Discovery.ServiceType.DATA);
                if (serverNodes == null || serverNodes.isEmpty()) {
                    LOG.warn("the cluster server is empty !!");
                    return;
                }
                // 2. create request queue
                Collection<Request> requests = new ConcurrentSkipListSet<>();
                serverNodes.stream().forEach(
                    x -> {
                        HttpUrl url = new HttpUrl.Builder()
                            .host(x.getHost())
                            .port(x.getPort())
                            .encodedPath("/resource").build();
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
                                continue;
                            }
                            ResponseBody body = response.body();
                            if (body == null) {
                                continue;
                            }
                            byte[] data = body.bytes();
                            if (data == null || data.length == 0) {
                                continue;
                            }
                            NodeSnapshotInfo snapshotInfo = JsonUtils.toObjectQuietly(data, NodeSnapshotInfo.class);
                            if (snapshotInfo == null) {
                                continue;
                            }
                            queue.add(snapshotInfo);
                            break;
                        } catch (IOException e) {
                            e.printStackTrace();
                        } finally {
                            retryCount--;
                            if (!runFlag) {
                                break;
                            }
                            try {
                                Thread.sleep(300);
                            } catch (InterruptedException e) {
                                e.printStackTrace();
                            }
                        }
                    }

                });

            }
        }, 0, intervalTime, TimeUnit.SECONDS);

        ScheduledFuture<?> future1 = pool.schedule(new Runnable() {
            @Override
            public void run() {
                NodeSnapshotInfo snapshotInfo;

                while (runFlag) {
                    try {
                        snapshotInfo = queue.take();
                        String id = snapshotInfo.getNodeId();
                        guiFileMaintainer.setNodeInfo(id, convertor.gatherNodeInfo(snapshotInfo));
                        guiFileMaintainer.setCpuInfo(id, convertor.gatherCpuInfo(snapshotInfo));
                        guiFileMaintainer.setLoadInfo(id, convertor.gatherLoadInfo(snapshotInfo));
                        guiFileMaintainer.setMemInfo(id, convertor.gahterMemInfo(snapshotInfo));
                        guiFileMaintainer.setDiskIOs(id, convertor.gatherDiskIOInfos(snapshotInfo));
                        guiFileMaintainer.setDiskUsages(id, convertor.gatherDiskUsageInfos(snapshotInfo));
                        guiFileMaintainer.setNetInfos(id, convertor.gatherNetInfos(snapshotInfo));
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }

            }
        }, 0, TimeUnit.SECONDS);

        futures.add(future);
        futures.add(future1);
    }

    @Override
    public void stop() throws Exception {
        this.runFlag = false;
        futures.stream().forEach(x -> {
            x.cancel(true);
        });
        if (this.pool != null) {
            this.pool.shutdownNow();
        }
        if (closer != null) {
            closer.close();
        }
    }
}
