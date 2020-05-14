package com.bonree.brfs.gui.server.stats;

import com.bonree.brfs.client.discovery.CachedDiscovery;
import com.bonree.brfs.client.discovery.Discovery;
import com.bonree.brfs.client.discovery.HttpDiscovery;
import com.bonree.brfs.client.discovery.ServerNode;
import com.bonree.brfs.client.json.JsonCodec;
import com.bonree.brfs.client.utils.DaemonThreadFactory;
import com.bonree.brfs.client.utils.HttpStatus;
import com.bonree.brfs.client.utils.SocketChannelSocketFactory;
import com.bonree.brfs.common.process.LifeCycle;
import com.bonree.brfs.common.resource.vo.NodeSnapshotInfo;
import com.bonree.brfs.common.statistic.ReadCountModel;
import com.bonree.brfs.common.statistic.ReadStatCollector;
import com.bonree.brfs.common.statistic.WriteCountModel;
import com.bonree.brfs.common.statistic.WriteStatCollector;
import com.bonree.brfs.common.utils.JsonUtils;
import com.bonree.brfs.common.utils.TimeUtils;
import com.bonree.brfs.gui.server.BrfsConfig;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.io.Closer;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.inject.Inject;
import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import javax.ws.rs.WebApplicationException;
import okhttp3.HttpUrl;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.ResponseBody;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StatisticCollector implements LifeCycle {
    private static final Logger LOG = LoggerFactory.getLogger(StatisticCollector.class);
    private static final Logger READ_LOG = LoggerFactory.getLogger("read_Statistic");
    private static final Logger WRITE_LOG = LoggerFactory.getLogger("write_Statistic");
    ObjectMapper readMapper = new ObjectMapper();
    JavaType readValue = readMapper.getTypeFactory().constructParametricType(HashMap.class, String.class,
                                                                             ReadCountModel.class);
    JavaType writeValue = readMapper.getTypeFactory().constructParametricType(HashMap.class, String.class,
                                                                              WriteCountModel.class);
    private Discovery httpDiscovery;
    static ReadStatCollector readStatCollector = new ReadStatCollector();
    static WriteStatCollector writeStatCollector = new WriteStatCollector();
    private StatisticFlusher statisticFlusher;
    private int intervalTime;
    BrfsConfig brfsConfig;
    private OkHttpClient client;
    private Closer closer;
    private ScheduledExecutorService pool;
    private List<ScheduledFuture<?>> futures = new ArrayList<>();
    private boolean runFlag = false;
    private BlockingQueue<ReadCountModel> readCountQ = new LinkedBlockingQueue<>();
    private BlockingQueue<NodeSnapshotInfo> writeCountQ = new LinkedBlockingQueue<>();

    @Inject
    public StatisticCollector(Discovery httpDiscovery,
                              StatConfigs config,
                              StatisticFlusher statisticFlusher,
                              BrfsConfig brfsConfig) {
        this.httpDiscovery = httpDiscovery;
        this.intervalTime = config.getIntervalTime();
        this.statisticFlusher = statisticFlusher;
        this.brfsConfig = brfsConfig;
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
            .setNameFormat("statisticCollector")
            .build();
        this.pool = Executors.newScheduledThreadPool(2, threadFactory);

        ScheduledFuture<?> future = this.pool.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                runFlag = true;
                // 1.get server list
                List<ServerNode> dataNodes = httpDiscovery.getServiceList(Discovery.ServiceType.DATA);
                List<ServerNode> regionNodes = httpDiscovery.getServiceList(Discovery.ServiceType.REGION);
                if (dataNodes == null || dataNodes.isEmpty()) {
                    LOG.warn("the dataNodes server is empty !!");
                    return;
                }
                if (regionNodes == null || regionNodes.isEmpty()) {
                    LOG.warn("the regionNodes server is empty !!");
                    return;
                }
                long currentStartTime = TimeUtils.prevTimeStamp(System.currentTimeMillis(), Duration.parse("PT1M").toMillis());
                long currentDay = TimeUtils.prevTimeStamp(currentStartTime, Duration.parse("P1D").toMillis());
                // 在这里循环拿各个节点的信息，
                for (ServerNode serverNode : dataNodes) {
                    Request httpRequest = new Request.Builder()
                        .url(HttpUrl.get(URI.create("http://" + serverNode.getHost() + ":" + brfsConfig.getDataNodePort()))
                                    .newBuilder()
                                    .encodedPath("/stat/read")
                                    .build())
                        .get()
                        .build();
                    try {
                        okhttp3.Response response = httpClient.newCall(httpRequest).execute();
                        if (response.code() == HttpStatus.CODE_OK) {
                            ResponseBody responseBody = response.body();
                            if (responseBody == null) {
                                continue;
                            }
                            Map<String, ReadCountModel> readResult;
                            String json = responseBody.string();
                            ObjectMapper mapper = new ObjectMapper();

                            readResult = mapper.readValue(json, readValue);
                            for (String s : readResult.keySet()) {
                                ReadCountModel readCountModel = readResult.get(s);
                                readStatCollector.addCount(s, readCountModel.getReadCount());
                            }
                        }
                    } catch (IOException e) {
                        LOG.error("errrrrrr", e);
                    }

                }

                for (ServerNode regionNode : regionNodes) {
                    Request httpRequest = new Request.Builder()
                        .url(HttpUrl.get(URI.create("http://" + regionNode.getHost() + ":" + regionNode.getPort()))
                                    .newBuilder()
                                    .encodedPath("/stat/write")
                                    .build())
                        .get()
                        .build();
                    try {
                        okhttp3.Response response = httpClient.newCall(httpRequest).execute();
                        if (response.code() == HttpStatus.CODE_OK) {
                            ResponseBody responseBody = response.body();
                            if (responseBody == null) {
                                continue;
                            }
                            Map<String, WriteCountModel> writeResult;
                            String json = responseBody.string();
                            ObjectMapper mapper = new ObjectMapper();

                            writeResult = mapper.readValue(json, writeValue);
                            for (String s : writeResult.keySet()) {
                                WriteCountModel writeCountModel = writeResult.get(s);
                                writeStatCollector.addCount(s, writeCountModel.getWriteCount());
                            }
                        }
                    } catch (IOException e) {
                        LOG.error("errrrrrr", e);
                    }

                }

                Map<String, ReadCountModel> readCountMap = readStatCollector.popAll();
                Map<String, WriteCountModel> writeCountMap = writeStatCollector.popAll();

                for (String s : readCountMap.keySet()) {
                    statisticFlusher.flush(currentDay, currentStartTime, s, readCountMap.get(s).getReadCount(), true);
                }
                for (String s : writeCountMap.keySet()) {
                    statisticFlusher.flush(currentDay, currentStartTime, s, writeCountMap.get(s).getWriteCount(), false);
                }
            }

        }, 0, intervalTime, TimeUnit.SECONDS);

        ScheduledFuture<?> future1 = pool.schedule(new Runnable() {
            @Override
            public void run() {
                ReadCountModel readCountModel;

                while (runFlag) {
                    try {
                        readCountModel = readCountQ.take();
                        String srName = readCountModel.getSrName();
                        // statisticFlusher.flush(srName, );
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

    public static void main1(String[] args) {

        OkHttpClient httpClient = new OkHttpClient.Builder()
            .socketFactory(new SocketChannelSocketFactory())
            .build();
        JsonCodec codec = new JsonCodec(new ObjectMapper());

        Discovery discovery = new CachedDiscovery(
            new HttpDiscovery(httpClient, new URI[] {URI.create("http://localhost:8100")}, codec),
            Executors.newSingleThreadExecutor(new DaemonThreadFactory("brfs-discovery-%s")),
            Duration.ofSeconds(30), Duration.ofSeconds(30));
        List<ServerNode> serviceList = discovery.getServiceList(Discovery.ServiceType.DATA);
        for (ServerNode serverNode : serviceList) {
            Request httpRequest = new Request.Builder()
                .url(HttpUrl.get(URI.create("http://" + serverNode.getHost() + ":" + 18002))
                            .newBuilder()
                            .encodedPath("/stat/read")
                            .build())
                .get()
                .build();
            try {
                okhttp3.Response response = httpClient.newCall(httpRequest).execute();
                if (response.code() == HttpStatus.CODE_OK) {
                    ResponseBody responseBody = response.body();
                    if (responseBody == null) {
                        continue;
                    }
                    Map<String, ReadCountModel> stringReadCountModelMap = new HashMap<String, ReadCountModel>();
                    try {
                        stringReadCountModelMap = JsonUtils.toObject(responseBody.string(), stringReadCountModelMap.getClass());
                    } catch (JsonUtils.JsonException e) {
                        e.printStackTrace();
                    }
                    System.out.println(responseBody.string());
                }
            } catch (IOException e) {
                throw new WebApplicationException(777);
            }

        }
    }

    public static void main(String[] args) throws JsonUtils.JsonException {
        String json = "{\"new_region8\":{\"count\":3,\"srName\":\"new_region8\"}};";
        System.out.println(json);
        Map map = JsonUtils.toObject(json, Map.class);
        System.out.println(1);
        String nul = "{}";
        Map map1 = JsonUtils.toObject(nul, Map.class);

    }

}
