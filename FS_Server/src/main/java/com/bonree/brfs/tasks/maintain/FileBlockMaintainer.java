package com.bonree.brfs.tasks.maintain;

import com.bonree.brfs.common.lifecycle.LifecycleStart;
import com.bonree.brfs.common.lifecycle.LifecycleStop;
import com.bonree.brfs.common.lifecycle.ManageLifecycle;
import com.bonree.brfs.common.process.LifeCycle;
import com.bonree.brfs.common.utils.BRFSFileUtil;
import com.bonree.brfs.common.utils.BRFSPath;
import com.bonree.brfs.common.utils.FileUtils;
import com.bonree.brfs.duplication.storageregion.StorageRegion;
import com.bonree.brfs.duplication.storageregion.StorageRegionManager;
import com.bonree.brfs.identification.LocalPartitionInterface;
import com.bonree.brfs.identification.SecondIdsInterface;
import com.bonree.brfs.partition.model.LocalPartitionInfo;
import com.bonree.brfs.rebalance.route.RouteLoader;
import com.bonree.brfs.rebalance.route.impl.RouteParser;
import com.bonree.brfs.schedulers.utils.InvaildFileBlockFilter;
import com.bonree.brfs.tasks.monitor.RebalanceTaskMonitor;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.inject.Inject;
import java.io.File;
import java.time.Duration;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/***
 * 文件块维护类，负责清理非法数据，不符合BRFS路径规则，不应当存储在本地合法文件
 */
@ManageLifecycle
public class FileBlockMaintainer implements LifeCycle {
    private static final Logger LOG = LoggerFactory.getLogger(FileBlockMaintainer.class);
    private ScheduledExecutorService pool = null;
    private LocalPartitionInterface localPartitionInterface;
    private RebalanceTaskMonitor monitor;
    private StorageRegionManager manager;
    private SecondIdsInterface secondIds;
    private RouteLoader loader;
    private long intervalTime;

    @Inject
    public FileBlockMaintainer(LocalPartitionInterface localPartitionInterface, RebalanceTaskMonitor monitor,
                               StorageRegionManager manager, SecondIdsInterface secondIds, RouteLoader loader) {
        this(localPartitionInterface, monitor, manager, secondIds, loader, 1);
    }

    public FileBlockMaintainer(LocalPartitionInterface localPartitionInterface, RebalanceTaskMonitor monitor,
                               StorageRegionManager manager, SecondIdsInterface secondIds, RouteLoader loader,
                               long intervalTime) {
        this.localPartitionInterface = localPartitionInterface;
        this.monitor = monitor;
        this.manager = manager;
        this.secondIds = secondIds;
        this.loader = loader;
        this.intervalTime = intervalTime;
    }

    @LifecycleStart
    @Override
    public void start() throws Exception {
        pool =
            Executors.newSingleThreadScheduledExecutor(new ThreadFactoryBuilder().setNameFormat("FileBlockMaintainer").build());
        pool.scheduleAtFixedRate(new FileBlockWorker(localPartitionInterface, monitor, manager, secondIds, loader), 0,
                                 intervalTime, TimeUnit.HOURS);
        LOG.info(" block server start");
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
        private final Logger log = LoggerFactory.getLogger(FileBlockWorker.class);
        private LocalPartitionInterface localPartitionInterface;
        private RebalanceTaskMonitor monitor;
        private StorageRegionManager manager;
        private SecondIdsInterface secondIds;
        private RouteLoader loader;

        public FileBlockWorker(LocalPartitionInterface localPartitionInterface, RebalanceTaskMonitor monitor,
                               StorageRegionManager manager, SecondIdsInterface secondIds, RouteLoader loader) {
            this.localPartitionInterface = localPartitionInterface;
            this.monitor = monitor;
            this.manager = manager;
            this.secondIds = secondIds;
            this.loader = loader;
        }

        @Override
        public void run() {
            try {
                if (!monitor.isExecute()) {
                    handleInvalidBlocks(secondIds, manager, localPartitionInterface, monitor, loader, System.currentTimeMillis());
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
                                        RouteLoader loader, long limitTime) {
            // 1. 获取storageRegion信息
            Collection<StorageRegion> sns = srManager.getStorageRegionList();
            // 2. 获取磁盘信息
            Collection<LocalPartitionInfo> localPartitionInfos = localPartitionInterface.getPartitions();
            // 3. 扫描文件块，并获取非法文件块路径
            Queue<String> invalidBlockQueue = scanInvalidBlocks(secondIds, monitor, sns, localPartitionInfos, loader, limitTime);
            // 4. 若见采集结果不为空则调用删除线程
            deleteInvalidBlock(invalidBlockQueue, monitor);

        }

        private Queue<String> scanInvalidBlocks(SecondIdsInterface secondIds, RebalanceTaskMonitor monitor,
                                                Collection<StorageRegion> sns, Collection<LocalPartitionInfo> localPartitionInfos,
                                                RouteLoader loader, long limitTime) {
            if (localPartitionInfos == null || localPartitionInfos.isEmpty()) {
                return null;
            }
            if (sns == null || sns.isEmpty()) {
                log.debug("skip search data because is empty");
                return null;
            }
            Queue<String> invalidBlockQueue = new ConcurrentLinkedQueue<String>();
            // sn 目录及文件
            int snId;
            RouteParser parser;
            Map<String, String> snMap;
            long granule;
            long snLimitTime;

            for (StorageRegion sn : sns) {
                // 单个副本的不做检查
                if (sn.getReplicateNum() <= 1) {
                    continue;
                }
                snId = sn.getId();
                granule = Duration.parse(sn.getFilePartitionDuration()).toMillis();
                ;
                snLimitTime = limitTime - limitTime % granule;
                log.info(" watch dog eat {} :{}", sn.getName(), sn.getId());

                parser = new RouteParser(snId, loader);
                // 使用前必须更新路由规则，否则会解析错误
                snMap = new HashMap<>();
                snMap.put(BRFSPath.STORAGEREGION, sn.getName());
                for (LocalPartitionInfo local : localPartitionInfos) {
                    if (monitor.isExecute()) {
                        invalidBlockQueue.clear();
                        return null;
                    }
                    List<String> paths = scanSinglePartition(sn, local, secondIds, parser, snMap, snLimitTime);
                    if (paths != null && !paths.isEmpty()) {
                        invalidBlockQueue.addAll(paths);
                    }
                }

            }
            return invalidBlockQueue;
        }

        private List<String> scanSinglePartition(StorageRegion storageRegion, LocalPartitionInfo localPartitionInfo,
                                                 SecondIdsInterface secondIdsInterface, RouteParser parser,
                                                 Map<String, String> snMap, long lastTime) {
            String dataDir = localPartitionInfo.getDataDir();
            String partitionId = localPartitionInfo.getPartitionId();
            String secondId = secondIdsInterface.getSecondId(partitionId, storageRegion.getId());
            InvaildFileBlockFilter filter = new InvaildFileBlockFilter(parser, storageRegion, secondId, lastTime);
            List<BRFSPath> invalidFileBlocks = BRFSFileUtil.scanBRFSFiles(dataDir, snMap, snMap.size(), filter);
            return invalidFileBlocks == null || invalidFileBlocks.isEmpty() ? null : invalidFileBlocks.stream().map(f -> {
                return dataDir + File.separator + f.toString();
            }).collect(Collectors.toList());
        }

        /**
         * 删除非法的文件块
         *
         * @param invalidBlocks
         * @param monitor
         */
        private int deleteInvalidBlock(Queue<String> invalidBlocks, RebalanceTaskMonitor monitor) {
            // 为空跳出
            if (invalidBlocks == null || invalidBlocks.isEmpty()) {
                log.debug("queue is empty skip !!!");
                return 0;
            }
            int count = 0;
            while (!invalidBlocks.isEmpty() && !monitor.isExecute()) {
                try {
                    String path = invalidBlocks.poll();
                    boolean deleteFlag = FileUtils.deleteFile(path);
                    log.debug("file : {} deleting!", path);
                    if (!deleteFlag) {
                        log.info("file : {} cann't delete !!!", path);
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
