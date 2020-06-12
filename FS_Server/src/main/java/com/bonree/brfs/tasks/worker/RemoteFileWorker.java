package com.bonree.brfs.tasks.worker;

import com.bonree.brfs.common.service.Service;
import com.bonree.brfs.common.utils.BRFSPath;
import com.bonree.brfs.common.utils.BrStringUtils;
import com.bonree.brfs.common.utils.TimeUtils;
import com.bonree.brfs.disknode.client.TcpDiskNodeClient;
import com.bonree.brfs.disknode.server.handler.data.FileInfo;
import com.bonree.brfs.duplication.storageregion.StorageRegion;
import com.bonree.brfs.identification.IDSManager;
import com.bonree.brfs.rebalance.route.BlockAnalyzer;
import com.bonree.brfs.rebalance.route.RouteCache;
import com.bonree.brfs.schedulers.utils.LocalByteStreamConsumer;
import com.bonree.brfs.schedulers.utils.TcpClientBuilder;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.io.File;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.commons.io.FilenameUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RemoteFileWorker {
    private static final Logger LOG = LoggerFactory.getLogger(RemoteFileWorker.class);
    private Service remoteServer;
    private IDSManager idsManager;
    private RouteCache routeCache;

    public RemoteFileWorker(Service remoteServer, IDSManager idsManager, RouteCache routeCache) {
        this.remoteServer = remoteServer;
        this.idsManager = idsManager;
        this.routeCache = routeCache;
    }

    public Collection<BRFSPath> listFiles(StorageRegion region) throws Exception {
        return listFiles(region, 0L, System.currentTimeMillis());
    }

    /**
     * 列出指定region 时间段的所有有效文件块，不包含正在写入的，
     *
     * @param region
     * @param startTime
     * @param endTime
     *
     * @return
     *
     * @throws InterruptedException
     */
    public Collection<BRFSPath> listFiles(StorageRegion region, long startTime, long endTime) throws Exception {
        // 1.获取要遍历的各个磁盘分区的目录集合
        Map<String, Collection<String>> dirPaths = buildDirPath(region, startTime, endTime);
        if (dirPaths.isEmpty()) {
            return ImmutableList.of();
        }
        TcpDiskNodeClient client = TcpClientBuilder.getInstance().getClient(remoteServer);
        Collection<BRFSPath> remoteFiles = new ArrayList<>();
        // 2.遍历并获取有效的文件集合
        dirPaths.forEach((secondId, dirs) -> {
            Collection<BRFSPath> files = collectFiles(client, region, dirs, secondId);
            if (files != null && !files.isEmpty()) {
                remoteFiles.addAll(files);
            }
        });
        if (remoteFiles.isEmpty()) {
            return ImmutableList.of();
        }
        return remoteFiles;
    }

    /**
     * 获取指定路径结合
     *
     * @param client
     * @param dirs
     * @param secondId
     *
     * @return
     */
    public Collection<BRFSPath> collectFiles(TcpDiskNodeClient client, StorageRegion region, Collection<String> dirs,
                                             String secondId) {
        if (dirs == null || dirs.isEmpty()) {
            return ImmutableList.of();
        }
        // 1. 获取所有目录的文件列表
        Collection<FileInfo> files = new ArrayList<>();
        dirs.stream().forEach(dir -> {
            List<FileInfo> childs = client.listFiles(dir, 1);
            if (childs != null) {
                files.addAll(childs);
            }
        });
        // 2.处理文件列表
        BlockAnalyzer parser = routeCache.getBlockAnalyzer(region.getId());
        Collection<BRFSPath> remotePaths = collectRemotePaths(parser, files, secondId);
        return remotePaths;
    }

    /**
     * 概述：转换集合为str集合
     *
     * @param files
     *
     * @return
     *
     * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
     */
    public Collection<BRFSPath> collectRemotePaths(BlockAnalyzer parser, Collection<FileInfo> files, String secondId) {
        Collection<BRFSPath> strs = new ArrayList<>();
        List<String> errorFiles = new ArrayList<>();
        String[] checks;
        for (FileInfo file : files) {
            if (file.getType() == FileInfo.TYPE_DIR) {
                continue;
            }
            String path = file.getPath();
            String fileName = FilenameUtils.getName(path);
            // 排除rd文件
            if (fileName.indexOf(".rd") > 0) {
                fileName = FilenameUtils.getBaseName(fileName);
                errorFiles.add(fileName);
                LOG.warn("file: [{}] contain rd file !! skip ", fileName);
                continue;
            }
            // 排除非法数据
            checks = BrStringUtils.getSplit(fileName, "_");
            if (checks == null || checks.length <= 1) {
                errorFiles.add(fileName);
                LOG.warn("file: [{}] is unlaw file !! skip ", fileName);
                continue;
            }
            if (isUnlaw(secondId, parser, fileName)) {
                LOG.warn("file: [{}] is not [{}] file", fileName, secondId);
                continue;
            }
            BRFSPath brfsPath = BRFSPath.parserFile(path);
            if (brfsPath != null) {
                strs.add(brfsPath);
            } else {
                LOG.warn("path {} conveto BRFS file error", path);
            }
        }
        if (strs.isEmpty()) {
            return ImmutableList.of();
        }
        strs.stream().filter(path -> {
            return errorFiles.contains(path.getFileName());
        }).collect(Collectors.toList());
        return strs;
    }

    private boolean isUnlaw(String sid, BlockAnalyzer parser, String fileName) {
        try {
            String[] alives = parser.searchVaildIds(fileName);
            if (alives == null || alives.length == 0) {
                LOG.warn("[{}] analys service error !! alives is null !!!", fileName);
                return true;
            }
            List<String> eles = Arrays.asList(alives);
            boolean status = !eles.contains(sid);
            if (status) {
                LOG.warn("file: [{}], server: [{}], serverlist :{}", fileName, sid, eles);
            }
            return status;
        } catch (Exception e) {
            LOG.error("check storageregion :{}, file {} happener error", fileName, sid, e);
        }
        return false;
    }

    /**
     * 构建远程路径，并按照二级serverid进行分类
     *
     * @param region
     * @param startTime
     * @param endTime
     *
     * @return
     */
    public Map<String, Collection<String>> buildDirPath(StorageRegion region, long startTime, long endTime) {
        long granule = Duration.parse(region.getFilePartitionDuration()).toMillis();
        long baseStartTime = startTime;
        if (startTime < region.getCreateTime()) {
            baseStartTime = region.getCreateTime();
        }
        Collection<Long> granuleTimes = convertDirDurationTime(baseStartTime, endTime, granule);
        if (granuleTimes.isEmpty()) {
            return ImmutableMap.of();
        }
        Collection<String> secondIds = idsManager.getSecondIds(remoteServer.getServiceId(), region.getId());
        if (secondIds.isEmpty()) {
            return ImmutableMap.of();
        }
        int replicateNum = region.getReplicateNum();
        Map<String, Collection<String>> pathMap = new HashMap<>();
        for (int i = 1; i <= replicateNum; i++) {
            int serverIndex = i;
            secondIds.forEach(
                secondId -> {
                    Collection<String> remoteDirPaths = buildRemoteDirPath(region, granuleTimes, secondId, granule, serverIndex);
                    pathMap.put(secondId, remoteDirPaths);
                }
            );
        }
        return pathMap;
    }

    public Collection<String> buildRemoteDirPath(StorageRegion region, Collection<Long> granuleTimes, String secondId,
                                                 long granule, int serverIndex) {
        Collection<String> paths = new HashSet<>();
        String fileBlockName = buildDirName(secondId, serverIndex);
        granuleTimes.forEach(
            granuleTime -> {
                String dirTime = TimeUtils.timeInterval(granuleTime, granule);
                String path = buildRelativePath(region, dirTime, fileBlockName, serverIndex);
                paths.add(path);
            });
        return paths;
    }

    /**
     * 根据粒度时间将查询范围时间切分
     *
     * @param startTime
     * @param endTime
     * @param granule
     *
     * @return
     */
    public Collection<Long> convertDirDurationTime(long startTime, long endTime, long granule) {
        long grauleStartTime = TimeUtils.prevTimeStamp(startTime, granule);
        long granuleStopTime = TimeUtils.nextTimeStamp(endTime, granule);
        Collection<Long> set = new HashSet<>();
        for (long current = grauleStartTime; current < granuleStopTime; current += granule) {
            set.add(current);
        }
        return set;
    }

    public String buildRelativePath(StorageRegion region, String timeDirName, String fileName, int serverIndex) {
        return new StringBuilder().append('/')
                                  .append(region.getName())
                                  .append('/')
                                  .append(serverIndex)
                                  .append(timeDirName)
                                  .append(fileName)
                                  .toString();
    }

    public String buildDirName(String secondId, int dirIndex) {
        StringBuilder dirBuilder = new StringBuilder();
        for (int i = 0; i < dirIndex; i++) {
            dirBuilder.append("0_");
        }
        dirBuilder.append(secondId);
        return dirBuilder.toString();
    }

    public Map<BRFSPath, BRFSPath> downloadFiles(Map<BRFSPath, BRFSPath> downloadShip, String root, long sleep) throws Exception {
        Map<BRFSPath, BRFSPath> errorMap = new HashMap<>();
        TcpDiskNodeClient client = TcpClientBuilder.getInstance().getClient(remoteServer);
        try {
            for (Map.Entry<BRFSPath, BRFSPath> ship : downloadShip.entrySet()) {
                BRFSPath remote = ship.getKey();
                BRFSPath local = ship.getValue();
                if (!downloadFile(client, remote, local, root)) {
                    errorMap.put(remote, local);
                }
                Thread.sleep(sleep);
            }
        } finally {
            client.close();
        }
        return errorMap;
    }

    /**
     * 概述：恢复数据文件
     *
     * @param remote 远程主机
     *
     * @return
     *
     * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
     */
    public boolean downloadFile(TcpDiskNodeClient client, BRFSPath remote, BRFSPath local, String root) {
        String remotePath = remote.toString();
        String localPath = root + File.separator + local.toString();
        boolean status = false;
        int count = 3;
        LocalByteStreamConsumer consumer = new LocalByteStreamConsumer(localPath);
        try {
            do {
                if (count != 3) {
                    Thread.sleep(1000);
                }
                client.readFile(remotePath, consumer);
                count--;
            } while (!(status = consumer.getResult().get(30, TimeUnit.SECONDS)) && count > 0);
        } catch (Exception e) {
            LOG.error("down load from {} file{} happen error", remoteServer.getHost(), remote, e);
        } finally {
            client.closeFile(remotePath);
        }
        return status;
    }
}
