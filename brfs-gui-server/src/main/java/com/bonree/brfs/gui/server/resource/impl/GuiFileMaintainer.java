package com.bonree.brfs.gui.server.resource.impl;

import com.bonree.brfs.common.lifecycle.LifecycleStart;
import com.bonree.brfs.common.utils.JsonUtils;
import com.bonree.brfs.common.utils.TimeUtils;
import com.bonree.brfs.gui.server.GuiResourceConfig;
import com.bonree.brfs.gui.server.resource.GuiResourceMaintainer;
import com.bonree.brfs.gui.server.resource.vo.GuiCpuInfo;
import com.bonree.brfs.gui.server.resource.vo.GuiDiskIOInfo;
import com.bonree.brfs.gui.server.resource.vo.GuiDiskUsageInfo;
import com.bonree.brfs.gui.server.resource.vo.GuiLoadInfo;
import com.bonree.brfs.gui.server.resource.vo.GuiMemInfo;
import com.bonree.brfs.gui.server.resource.vo.GuiNetInfo;
import com.bonree.brfs.gui.server.resource.vo.GuiNodeInfo;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.inject.Inject;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.IOFileFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GuiFileMaintainer implements GuiResourceMaintainer {
    private static final Logger LOG = LoggerFactory.getLogger(GuiFileMaintainer.class);
    private static final String NODE_INFO = "node";
    private static final String CPU_INFO = "cpu";
    private static final String MEM_INFO = "mem";
    private static final String LOAD_INFO = "load";
    private static final String DISK_IO_INFO = "disk_io";
    private static final String DISK_USAGE_INFO = "disk_usage";
    private static final String NET_SPEED_INFO = "net_speed";
    private String basePath;
    private Collection<String> scanPaths = null;
    private ScheduledExecutorService pool = null;
    private int intervalTime;
    private int ttlTime;
    private ScheduledFuture<?> future;
    private boolean start;

    @Inject
    public GuiFileMaintainer(GuiResourceConfig config) {
        this.basePath = config.getGuiDir();
        this.intervalTime = config.getScanIntervalTime();
        this.ttlTime = config.getTtlTime();
    }

    private String createPath(String type, String node) {
        return new StringBuilder(this.basePath)
            .append(File.separator)
            .append(type)
            .append(File.separator)
            .append(node).toString();
    }

    /**
     * 加载数据
     *
     * @param dirPath
     * @param time
     * @param clazz
     * @param <T>
     *
     * @return
     */
    private <T> Collection<T> collectObject(String dirPath, long time, Class clazz) {
        File dir = new File(dirPath);
        String timeStamp = formateTime(time);
        Collection<T> results = new ArrayList<>();
        Collection<File> files = collectBelongFile(dir, timeStamp);
        for (File file : files) {
            byte[] data = readFile(file);
            if (data == null || data.length == 0) {
                continue;
            }
            Object obj = JsonUtils.toObjectQuietly(data, clazz);
            if (obj != null) {
                results.add((T) obj);
            }
        }
        return results;
    }

    protected <T> Collection<T> collectObjects(String dirPath, long time, Class clazz) {
        File dir = new File(dirPath);
        String timeStamp = formateTime(time);
        Collection<T> results = new ArrayList<>();
        Collection<File> files = collectBelongFile(dir, timeStamp);
        for (File file : files) {
            byte[] data = readFile(file);
            if (data == null || data.length == 0) {
                continue;
            }
            T[] objs = (T[]) JsonUtils.toObjectQuietly(data, clazz);
            if (objs != null && objs.length != 0) {
                for (T t : objs) {
                    results.add(t);
                }
            }
        }
        return results;
    }

    private void saveObject(String path, byte[] data) {
        if (data == null || data.length == 0) {
            return;
        }
        File file = new File(path);
        try {
            FileUtils.writeByteArrayToFile(file, data, false);
        } catch (IOException e) {
            LOG.error("save happen error content: {}", data == null ? "" : new String(data), e);
        }
    }

    /**
     * 读取文件
     *
     * @param file
     *
     * @return
     */
    private byte[] readFile(File file) {
        try {
            if (!file.exists()) {
                return null;
            }
            return FileUtils.readFileToByteArray(file);
        } catch (IOException e) {
            LOG.error("read [{}] happen error", file.getName(), e);
        }
        return null;
    }

    private String formateTime(long time) {
        return TimeUtils.formatTimeStamp(time, "yyyyMMddHHmm");
    }

    /**
     * 查找指定时间以前的文件
     *
     * @param dir
     * @param belongTime
     *
     * @return
     */
    private Collection<File> collectBelongFile(File dir, String belongTime) {
        try {
            if (!dir.exists()) {
                return new ArrayList<>();
            }
            return FileUtils.listFiles(dir, new IOFileFilter() {
                @Override
                public boolean accept(File file) {
                    return belongTime.compareTo(file.getName()) <= 0;
                }

                @Override
                public boolean accept(File file, String s) {
                    return true;
                }
            }, new IOFileFilter() {
                @Override
                public boolean accept(File file) {
                    return false;
                }

                @Override
                public boolean accept(File file, String s) {
                    return false;
                }
            });
        } catch (Exception e) {
            LOG.error("query {} happen error !!", dir.getName(), e);
        }
        return new ArrayList<>();
    }

    /**
     * 查找过期文件
     *
     * @param dir
     * @param ttlTime
     *
     * @return
     */
    private Collection<File> collectTTLFile(File dir, String ttlTime) {
        try {
            if (!dir.exists()) {
                return new ArrayList<>();
            }
            return FileUtils.listFiles(dir, new IOFileFilter() {
                @Override
                public boolean accept(File file) {
                    return ttlTime.compareTo(file.getName()) > 0;
                }

                @Override
                public boolean accept(File file, String s) {
                    return true;
                }
            }, new IOFileFilter() {
                @Override
                public boolean accept(File file) {
                    return true;
                }

                @Override
                public boolean accept(File file, String s) {
                    return true;
                }
            });
        } catch (Exception e) {
            LOG.error("scan ttl file {} happen error !!", dir.getName(), e);
        }
        return new ArrayList<>();
    }

    @Override
    public GuiNodeInfo getNodeInfo(String node) {
        File file = new File(createPath(NODE_INFO, node));
        if (!file.exists()) {
            return null;
        }
        byte[] data = readFile(file);
        if (data == null || data.length == 0) {
            return null;
        }
        return JsonUtils.toObjectQuietly(data, GuiNodeInfo.class);
    }

    @Override
    public Collection<GuiNodeInfo> getNodeInfos() {
        File rootDir = new File(createRoot(NODE_INFO));
        if (!rootDir.exists()) {
            return ImmutableList.of();
        }
        File[] childs = rootDir.listFiles();
        if (childs == null || childs.length == 0) {
            return ImmutableList.of();
        }
        Collection<GuiNodeInfo> list = new ArrayList<>();
        for (File child : childs) {
            String name = child.getName();
            GuiNodeInfo node = getNodeInfo(name);
            if (node == null) {
                continue;
            }
            list.add(node);
        }
        return list;
    }

    @Override
    public void setNodeInfo(String id, GuiNodeInfo nodeInfo) {
        File node = new File(createPath(NODE_INFO, id));
        if (node.exists()) {
            FileUtils.deleteQuietly(node);
        }
        byte[] data = JsonUtils.toJsonBytesQuietly(nodeInfo);
        if (data == null || data.length == 0) {
            LOG.error("converto byte[] happen error !![{}]", node);
            return;
        }
        try {
            FileUtils.writeByteArrayToFile(node, data, false);
        } catch (IOException e) {
            LOG.error("save to file happen error !![{}]", node, e);
        }
    }

    @Override
    public Collection<GuiCpuInfo> getCpuInfos(String id, long time) {
        return collectObject(createPath(CPU_INFO, id), time, GuiCpuInfo.class);
    }

    @Override
    public void setCpuInfo(String id, GuiCpuInfo cpuInfo) {
        if (cpuInfo == null) {
            return;
        }
        String timeStamp = formateTime(cpuInfo.getTime());
        byte[] data = JsonUtils.toJsonBytesQuietly(cpuInfo);
        saveObject(createPath(CPU_INFO, id) + File.separator + timeStamp, data);
    }

    @Override
    public Collection<GuiMemInfo> getMemInfos(String id, long time) {
        return collectObject(createPath(MEM_INFO, id), time, GuiMemInfo.class);
    }

    @Override
    public void setMemInfo(String id, GuiMemInfo memInfo) {
        if (memInfo == null) {
            return;
        }
        String timeStamp = formateTime(memInfo.getTime());
        byte[] data = JsonUtils.toJsonBytesQuietly(memInfo);
        saveObject(createPath(MEM_INFO, id) + File.separator + timeStamp, data);
    }

    @Override
    public Collection<GuiLoadInfo> getLoadInfos(String id, long time) {
        return collectObject(createPath(LOAD_INFO, id), time, GuiLoadInfo.class);
    }

    @Override
    public void setLoadInfo(String id, GuiLoadInfo loadInfo) {
        if (loadInfo == null) {
            return;
        }
        String timeStamp = formateTime(loadInfo.getTime());
        byte[] data = JsonUtils.toJsonBytesQuietly(loadInfo);
        saveObject(createPath(LOAD_INFO, id) + File.separator + timeStamp, data);
    }

    @Override
    public Map<String, Collection<GuiDiskIOInfo>> getDiskIOInfos(String id, long time) {
        Collection<GuiDiskIOInfo> array = collectObjects(createPath(DISK_IO_INFO, id), time, GuiDiskIOInfo[].class);
        if (array == null || array.isEmpty()) {
            return new HashMap<>();
        }
        Map<String, Collection<GuiDiskIOInfo>> map = new ConcurrentHashMap<>();
        array.stream().forEach(y -> {
            String diskId = y.getDiskId();
            if (map.get(diskId) == null) {
                map.put(diskId, new ArrayList<>());
            }
            map.get(diskId).add(y);
        });
        return map;
    }

    @Override
    public void setDiskIOs(String id, Collection<GuiDiskIOInfo> diskIOs) {
        if (diskIOs == null || diskIOs.isEmpty()) {
            return;
        }
        GuiDiskIOInfo tmp = diskIOs.stream().findAny().get();
        String name = formateTime(tmp.getTime());
        byte[] data = JsonUtils.toJsonBytesQuietly(diskIOs);
        saveObject(createPath(DISK_IO_INFO, id) + File.separator + name, data);
    }

    @Override
    public Map<String, Collection<GuiDiskUsageInfo>> getDiskUsages(String id, long time) {
        Collection<GuiDiskUsageInfo> array = collectObjects(createPath(DISK_USAGE_INFO, id), time, GuiDiskUsageInfo[].class);
        if (array == null || array.isEmpty()) {
            return new HashMap<>();
        }
        Map<String, Collection<GuiDiskUsageInfo>> map = new ConcurrentHashMap<>();
        array.stream().forEach(y -> {
            String diskId = y.getDiskId();
            if (map.get(diskId) == null) {
                map.put(diskId, new ArrayList<>());
            }
            map.get(diskId).add(y);
        });
        return map;
    }

    @Override
    public void setDiskUsages(String id, Collection<GuiDiskUsageInfo> usages) {
        if (usages == null || usages.isEmpty()) {
            return;
        }
        GuiDiskUsageInfo tmp = usages.stream().findAny().get();
        String name = formateTime(tmp.getTime());
        byte[] data = JsonUtils.toJsonBytesQuietly(usages);
        saveObject(createPath(DISK_USAGE_INFO, id) + File.separator + name, data);
    }

    @Override
    public Map<String, Collection<GuiNetInfo>> getNetInfos(String id, long time) {
        Collection<GuiNetInfo> array = collectObjects(createPath(NET_SPEED_INFO, id), time, GuiNetInfo[].class);
        if (array == null || array.isEmpty()) {
            return new HashMap<>();
        }
        Map<String, Collection<GuiNetInfo>> map = new ConcurrentHashMap<>();
        array.stream().forEach(y -> {
            String diskId = y.getNetDev();
            if (map.get(diskId) == null) {
                map.put(diskId, new ArrayList<>());
            }
            map.get(diskId).add(y);
        });

        return map;
    }

    @Override
    public void setNetInfos(String id, Collection<GuiNetInfo> netInfos) {
        if (netInfos == null || netInfos.isEmpty()) {
            return;
        }
        GuiNetInfo tmp = netInfos.stream().findAny().get();
        String name = formateTime(tmp.getTime());
        byte[] data = JsonUtils.toJsonBytesQuietly(netInfos);
        saveObject(createPath(NET_SPEED_INFO, id) + File.separator + name, data);
    }

    private String createRoot(String type) {
        return new StringBuilder(this.basePath).append(File.separator).append(type).toString();
    }

    @LifecycleStart
    @Override
    public void start() throws Exception {
        scanPaths = Arrays.asList(
            createRoot(CPU_INFO),
            createRoot(MEM_INFO),
            createRoot(LOAD_INFO),
            createRoot(DISK_IO_INFO),
            createRoot(DISK_USAGE_INFO),
            createRoot(NET_SPEED_INFO));
        pool = Executors.newScheduledThreadPool(2, new ThreadFactoryBuilder().setNameFormat("GuiFileWorker").build());
        start = true;
        future = pool.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                scanPaths.stream().forEach(y -> {
                    if (!start) {
                        LOG.info("stop it ");
                        return;
                    }
                    File rootDir = new File(y);
                    if (!rootDir.exists()) {
                        LOG.warn("{} not exists ", rootDir.getName());
                        return;
                    }
                    if (!rootDir.isDirectory()) {
                        rootDir.delete();
                        LOG.warn("{} not dir delete it ", rootDir.getName());
                        return;
                    }
                    long time = System.currentTimeMillis() - ttlTime;
                    String timeStr = GuiFileMaintainer.this.formateTime(time);
                    File[] childs = rootDir.listFiles();
                    if (childs == null || childs.length == 0) {
                        return;
                    }
                    Collection<File> sumLoss = new ArrayList<>();
                    for (File file : childs) {
                        Collection<File> loss = GuiFileMaintainer.this.collectTTLFile(file, timeStr);
                        if (loss != null && !loss.isEmpty()) {
                            sumLoss.addAll(loss);
                        }
                    }
                    if (sumLoss != null && !sumLoss.isEmpty() && start) {
                        sumLoss.stream().forEach(FileUtils::deleteQuietly);
                    }
                    LOG.info("{} delete {} file", rootDir.getName(), sumLoss.size());
                });
            }
        }, 0, intervalTime, TimeUnit.SECONDS);
    }

    @Override
    public void stop() throws Exception {
        start = false;
        if (future != null) {
            future.cancel(true);
        }
        if (pool != null) {
            pool.shutdown();
        }
    }
}
