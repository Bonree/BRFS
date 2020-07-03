package com.bonree.brfs.tasks.maintain;

import com.bonree.brfs.common.lifecycle.LifecycleStart;
import com.bonree.brfs.common.lifecycle.LifecycleStop;
import com.bonree.brfs.common.lifecycle.ManageLifecycle;
import com.bonree.brfs.common.process.LifeCycle;
import com.bonree.brfs.common.resource.vo.LocalPartitionInfo;
import com.bonree.brfs.common.utils.BRFSFileUtil;
import com.bonree.brfs.common.utils.BRFSPath;
import com.bonree.brfs.common.utils.TimeUtils;
import com.bonree.brfs.disknode.TaskConfig;
import com.bonree.brfs.duplication.storageregion.StorageRegion;
import com.bonree.brfs.duplication.storageregion.StorageRegionManager;
import com.bonree.brfs.identification.LocalPartitionInterface;
import com.bonree.brfs.identification.SecondIdsInterface;
import com.bonree.brfs.rebalance.route.BlockAnalyzer;
import com.bonree.brfs.rebalance.route.RouteCache;
import com.bonree.brfs.schedulers.utils.InvaildFileBlockFilter;
import com.bonree.brfs.tasks.monitor.RebalanceTaskMonitor;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.inject.Inject;
import java.io.File;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/***
 * 文件块维护类，负责清理非法数据，不符合BRFS路径规则，不应当存储在本地非法文件
 */
@ManageLifecycle
public class FileBlockMaintainer implements LifeCycle {
    private static final Logger LOG = LoggerFactory.getLogger(FileBlockMaintainer.class);
    private ScheduledExecutorService pool = null;
    private LocalPartitionInterface localPartitionInterface;
    private RebalanceTaskMonitor monitor;
    private StorageRegionManager manager;
    private SecondIdsInterface secondIds;
    private RouteCache routeCache;
    private String scanTime;
    private long intervalTime;
    private int startDelay;

    @Inject
    public FileBlockMaintainer(LocalPartitionInterface localPartitionInterface, RebalanceTaskMonitor monitor,
                               StorageRegionManager manager, SecondIdsInterface secondIds, RouteCache cache,
                               TaskConfig taskConfig) {
        this(localPartitionInterface,
             monitor,
             manager,
             secondIds,
             cache,
             taskConfig.getFileBlockScanTime(),
             taskConfig.getFileBlockScanIntervalMinute(),
             taskConfig.getStartdelayMinute());
    }

    protected FileBlockMaintainer() {
    }

    public FileBlockMaintainer(LocalPartitionInterface localPartitionInterface, RebalanceTaskMonitor monitor,
                               StorageRegionManager manager, SecondIdsInterface secondIds, RouteCache cache,
                               String scanTime, long intervalTime, int startDelay) {
        this.localPartitionInterface = localPartitionInterface;
        this.monitor = monitor;
        this.manager = manager;
        this.secondIds = secondIds;
        this.routeCache = cache;
        this.scanTime = scanTime;
        this.intervalTime = intervalTime;
        this.startDelay = startDelay;
    }

    @LifecycleStart
    @Override
    public void start() throws Exception {
        pool =
            Executors.newSingleThreadScheduledExecutor(new ThreadFactoryBuilder().setNameFormat("FileBlockMaintainer").build());
        int delayTime = getDelayTime(this.scanTime);
        FileBlockWorker worker = new FileBlockWorker(localPartitionInterface, monitor, manager, secondIds, routeCache, LOG);
        // 延迟1分钟启动确保路由规则加载完成
        pool.scheduleAtFixedRate(worker, delayTime, intervalTime, TimeUnit.MINUTES);
        pool.schedule(worker, this.startDelay, TimeUnit.MINUTES);
        LOG.info("block server start {} interval :{} minute", this.scanTime, this.intervalTime);
    }

    public int getDelayTime(String scanTime) {
        int startTime = convertScanTime(scanTime);
        if (startTime < 0) {
            return 1;
        }
        int currentMinute = currentMinute();
        if (startTime - currentMinute == 0) {
            return 0;
        } else if (startTime - currentMinute > 0) {
            return startTime - currentMinute;
        } else {
            return startTime + 1440 - currentMinute;
        }
    }

    public int currentMinute() {
        Calendar current = Calendar.getInstance();
        int hour = current.get(Calendar.HOUR_OF_DAY);
        int minute = current.get(Calendar.MINUTE);
        return hour * 60 + minute;
    }

    public int convertScanTime(String scanTime) {
        try {
            String[] fields = StringUtils.split(scanTime, ":");
            if (fields.length != 2) {
                return -1;
            }
            int hour = Integer.parseInt(fields[0]);
            int minute = Integer.parseInt(fields[1]);
            return hour * 60 + minute;
        } catch (Exception ignore) {
            //
        }
        return -1;
    }

    @LifecycleStop
    @Override
    public void stop() throws Exception {
        if (pool != null) {
            pool.shutdownNow();
        }
        LOG.info(" block server stop");
    }

    private class FileBlockWorker implements Runnable {
        private Logger log;
        private LocalPartitionInterface localPartitionInterface;
        private RebalanceTaskMonitor monitor;
        private StorageRegionManager manager;
        private SecondIdsInterface secondIds;
        private RouteCache cache;

        public FileBlockWorker(LocalPartitionInterface localPartitionInterface, RebalanceTaskMonitor monitor,
                               StorageRegionManager manager, SecondIdsInterface secondIds, RouteCache cache, Logger log) {
            this.localPartitionInterface = localPartitionInterface;
            this.monitor = monitor;
            this.manager = manager;
            this.secondIds = secondIds;
            this.cache = cache;
            this.log = log;
        }

        @Override
        public void run() {
            try {

                if (!monitor.isExecute()) {
                    handleInvalidBlocks(secondIds, manager, localPartitionInterface, monitor, cache, System.currentTimeMillis());
                } else {
                    LOG.info("scan file block worker skip run ! because there is rebalance task run !");
                }
            } catch (Exception e) {
                log.error("FileBlockWorker scan blockes happen error ", e);
            }
        }

        /**
         * 删除
         *
         * @param secondIds
         * @param srManager
         * @param localPartitionInterface
         * @param monitor
         * @param limitTime
         */
        public void handleInvalidBlocks(SecondIdsInterface secondIds, StorageRegionManager srManager,
                                        LocalPartitionInterface localPartitionInterface, RebalanceTaskMonitor monitor,
                                        RouteCache cache, long limitTime) {
            // 1. 获取storageRegion信息
            Collection<StorageRegion> sns = srManager.getStorageRegionList();
            // 2. 获取磁盘信息
            Collection<LocalPartitionInfo> localPartitionInfos = localPartitionInterface.getPartitions();
            // 3. 扫描文件块，并获取非法文件块路径
            Queue<File> invalidBlockQueue = scanInvalidBlocks(secondIds, monitor, sns, localPartitionInfos, cache, limitTime);
            // 4. 若见采集结果不为空则调用删除线程
            int count = deleteInvalidBlock(invalidBlockQueue, monitor);
            LOG.info("handler invalid file block num :{}", count);

        }

        private Queue<File> scanInvalidBlocks(SecondIdsInterface secondIds, RebalanceTaskMonitor monitor,
                                              Collection<StorageRegion> sns, Collection<LocalPartitionInfo> localPartitionInfos,
                                              RouteCache cache, long limitTime) {
            if (localPartitionInfos == null || localPartitionInfos.isEmpty()) {
                return null;
            }
            if (sns == null || sns.isEmpty()) {
                log.debug("skip search data because is empty");
                return null;
            }
            Queue<File> invalidBlockQueue = new ConcurrentLinkedQueue<File>();
            // sn 目录及文件
            int snId;
            BlockAnalyzer parser;
            Map<String, String> snMap;
            long granule;
            long snLimitTime;

            List<String> storageRegionNames = sns.stream().map(StorageRegion::getName).collect(Collectors.toList());
            for (LocalPartitionInfo local : localPartitionInfos) {
                // 处理sr文件
                for (StorageRegion sn : sns) {
                    // 单个副本的不做检查
                    if (sn.getReplicateNum() <= 1) {
                        continue;
                    }
                    snId = sn.getId();
                    granule = Duration.parse(sn.getFilePartitionDuration()).toMillis();
                    ;
                    snLimitTime = limitTime - limitTime % granule;
                    log.info("scan {} before {}", sn.getName(), TimeUtils.formatTimeStamp(snLimitTime, "yyyy-MM-dd HH:mm:ss"));

                    parser = cache.getBlockAnalyzer(snId);
                    // 使用前必须更新路由规则，否则会解析错误
                    snMap = new HashMap<>();
                    snMap.put(BRFSPath.STORAGEREGION, sn.getName());
                    if (monitor.isExecute()) {
                        invalidBlockQueue.clear();
                        return null;
                    }
                    List<File> paths = scanSinglePartition(sn, local, secondIds, parser, snMap, snLimitTime);
                    if (paths != null && !paths.isEmpty()) {
                        invalidBlockQueue.addAll(paths);
                    }
                }
                List<File> invalids = scanInvalidFile(storageRegionNames, local);
                if (invalids != null && !invalids.isEmpty()) {
                    invalidBlockQueue.addAll(invalids);
                }

            }

            return invalidBlockQueue;
        }

        /**
         * 扫描非法的目录
         *
         * @param storageRegions
         * @param local
         *
         * @return
         */
        private List<File> scanInvalidFile(Collection<String> storageRegions, LocalPartitionInfo local) {
            String dataDir = local.getDataDir();
            File root = new File(dataDir);
            if (!root.exists()) {
                return ImmutableList.of();
            }

            File[] files = root.listFiles();
            if (files == null || files.length == 0) {
                return ImmutableList.of();
            }
            List<File> array = new ArrayList<>();
            for (File file : files) {
                if (!storageRegions.contains(file.getName())) {
                    array.add(file);
                    LOG.info("partition [{}] find invalid file {}", local.getPartitionId(), file.getName());
                }
            }
            return array;
        }

        /**
         * 扫描单个磁盘分区的sr信息
         *
         * @param storageRegion
         * @param localPartitionInfo
         * @param secondIdsInterface
         * @param analyzer
         * @param snMap
         * @param lastTime
         *
         * @return
         */
        private List<File> scanSinglePartition(StorageRegion storageRegion, LocalPartitionInfo localPartitionInfo,
                                               SecondIdsInterface secondIdsInterface, BlockAnalyzer analyzer,
                                               Map<String, String> snMap, long lastTime) {
            String dataDir = localPartitionInfo.getDataDir();
            String partitionId = localPartitionInfo.getPartitionId();
            String secondId = secondIdsInterface.getSecondId(partitionId, storageRegion.getId());
            InvaildFileBlockFilter filter = new InvaildFileBlockFilter(analyzer, storageRegion, secondId, lastTime);
            List<BRFSPath> invalidFileBlocks = BRFSFileUtil.scanBRFSFiles(dataDir, snMap, snMap.size(), filter);
            List<File> files = invalidFileBlocks == null || invalidFileBlocks.isEmpty() ? new ArrayList<>() :
                invalidFileBlocks.stream().map(f -> {
                    return new File(dataDir + File.separator + f.toString());
                }).collect(Collectors.toList());

            return files;
        }

        /**
         * 删除非法的文件块
         *
         * @param invalidBlocks
         * @param monitor
         */
        private int deleteInvalidBlock(Queue<File> invalidBlocks, RebalanceTaskMonitor monitor) {
            // 为空跳出
            if (invalidBlocks == null || invalidBlocks.isEmpty()) {
                log.info("queue is empty skip !!!");
                return 0;
            }
            int count = 0;
            while (!invalidBlocks.isEmpty() && !monitor.isExecute()) {
                try {
                    File file = invalidBlocks.poll();
                    boolean deleteFlag = true;
                    if (file.isDirectory()) {
                        FileUtils.deleteDirectory(file);
                    } else {
                        deleteFlag = FileUtils.deleteQuietly(file);
                    }
                    log.debug("file : {} deleting!", file.getAbsolutePath());
                    if (!deleteFlag) {
                        log.info("file : {} cann't delete !!!", file.getAbsolutePath());
                    }
                    count++;
                    if (count % 100 == 0) {
                        Thread.sleep(1000L);
                    }
                } catch (Exception e) {
                    log.error("watch dog delete file error {}", e);
                }
            }
            // 若中断则清除已经扫描的文件块
            if (monitor.isExecute()) {
                invalidBlocks.clear();
            }
            return count;
        }
    }
}
