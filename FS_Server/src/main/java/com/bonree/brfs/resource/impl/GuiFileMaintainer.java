package com.bonree.brfs.resource.impl;

import com.bonree.brfs.common.lifecycle.LifecycleStart;
import com.bonree.brfs.common.lifecycle.LifecycleStop;
import com.bonree.brfs.common.lifecycle.ManageLifecycle;
import com.bonree.brfs.common.process.LifeCycle;
import com.bonree.brfs.common.utils.JsonUtils;
import com.bonree.brfs.common.utils.TimeUtils;
import com.bonree.brfs.disknode.GuiResourceConfig;
import com.bonree.brfs.resource.GuiResourceMaintainer;
import com.bonree.brfs.resource.vo.GuiCpuInfo;
import com.bonree.brfs.resource.vo.GuiDiskIOInfo;
import com.bonree.brfs.resource.vo.GuiDiskUsageInfo;
import com.bonree.brfs.resource.vo.GuiLoadInfo;
import com.bonree.brfs.resource.vo.GuiMemInfo;
import com.bonree.brfs.resource.vo.GuiNetInfo;
import com.bonree.brfs.resource.vo.GuiNodeInfo;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
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
import java.util.concurrent.TimeUnit;
import javax.inject.Inject;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.IOFileFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ManageLifecycle
public class GuiFileMaintainer implements GuiResourceMaintainer, LifeCycle {
    private static final Logger LOG = LoggerFactory.getLogger(GuiFileMaintainer.class);
    private String basePath;
    private String nodeFilePath;
    private String cpuDirPath;
    private String memDirPath;
    private String loadDirPath;
    private String diskIODirPath;
    private String diskUsageDirPath;
    private String netInfoDirPath;
    private Collection<String> scanPaths = null;
    private ScheduledExecutorService pool = null;
    private int intervalTime;
    private int ttlTime;
    private boolean run =false;
    private boolean start;

    @Inject
    public GuiFileMaintainer(GuiResourceConfig config) {
        this.basePath = config.getGuiDir();
        this.run = config.isRunFlag();
        nodeFilePath = this.basePath + File.separator + "sys/node";
        cpuDirPath = this.basePath + File.separator + "cpu";
        memDirPath = this.basePath + File.separator + "mem";
        loadDirPath = this.basePath + File.separator + "load";
        diskIODirPath = this.basePath + File.separator + "disk/io";
        diskUsageDirPath = this.basePath + File.separator + "disk/usage";
        netInfoDirPath = this.basePath + File.separator + "net/stat";
        this.intervalTime = config.getScanIntervalTime();
        this.ttlTime = config.getTtlTime();

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
            LOG.error("save happen error content: {}", new String(data), e);
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
    public GuiNodeInfo getNodeInfo() {
        File node = new File(nodeFilePath);
        if (!node.exists()) {
            return null;
        }
        byte[] data = readFile(node);
        if (data == null || data.length == 0) {
            return null;
        }
        return JsonUtils.toObjectQuietly(data, GuiNodeInfo.class);
    }

    @Override
    public void setNodeInfo(GuiNodeInfo nodeInfo) {
        File node = new File(nodeFilePath);
        if (node.exists()) {
            FileUtils.deleteQuietly(node);
        }
        byte[] data = JsonUtils.toJsonBytesQuietly(node);
        if (data == null || data.length == 0) {
            LOG.error("converto byte[] happen error !![{}]", node);
            return;
        }
        try {
            FileUtils.writeByteArrayToFile(node, data, false);
        } catch (IOException e) {
            LOG.error("save to file happen error !![{}]", node);
        }
    }

    @Override
    public Collection<GuiCpuInfo> getCpuInfos(long time) {
        return collectObject(cpuDirPath, time, GuiCpuInfo.class);
    }

    @Override
    public void setCpuInfo(GuiCpuInfo cpuInfo) {
        if (cpuInfo == null) {
            return;
        }
        String timeStamp = formateTime(cpuInfo.getTime());
        byte[] data = JsonUtils.toJsonBytesQuietly(cpuInfo);
        saveObject(cpuDirPath + File.separator + timeStamp, data);
    }

    @Override
    public Collection<GuiMemInfo> getMemInfos(long time) {
        return collectObject(memDirPath, time, GuiMemInfo.class);
    }

    @Override
    public void setMemInfo(GuiMemInfo memInfo) {
        if (memInfo == null) {
            return;
        }
        String timeStamp = formateTime(memInfo.getTime());
        byte[] data = JsonUtils.toJsonBytesQuietly(memInfo);
        saveObject(memDirPath + File.separator + timeStamp, data);
    }

    @Override
    public Collection<GuiLoadInfo> getLoadInfos(long time) {
        return collectObject(loadDirPath, time, GuiLoadInfo.class);
    }

    @Override
    public void setLoadInfo(GuiLoadInfo loadInfo) {
        if (loadInfo == null) {
            return;
        }
        String timeStamp = formateTime(loadInfo.getTime());
        byte[] data = JsonUtils.toJsonBytesQuietly(loadInfo);
        saveObject(loadDirPath + File.separator + timeStamp, data);
    }

    @Override
    public Map<String, Collection<GuiDiskIOInfo>> getDiskIOInfos(long time) {
        Collection<GuiDiskIOInfo> array = collectObjects(diskIODirPath, time, GuiNetInfo[].class);
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
    public void setDiskIOs(Collection<GuiDiskIOInfo> diskIOs) {
        if (diskIOs == null || diskIOs.isEmpty()) {
            return;
        }
        GuiDiskIOInfo tmp = diskIOs.stream().findAny().get();
        String name = formateTime(tmp.getTime());
        byte[] data = JsonUtils.toJsonBytesQuietly(diskIOs);
        saveObject(diskUsageDirPath + File.separator + name, data);
    }

    @Override
    public Map<String, Collection<GuiDiskUsageInfo>> getDiskUsages(long time) {
        Collection<GuiDiskUsageInfo> array = collectObjects(diskUsageDirPath, time, GuiNetInfo[].class);
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
    public void setDiskUsages(Collection<GuiDiskUsageInfo> usages) {
        if (usages == null || usages.isEmpty()) {
            return;
        }
        GuiDiskUsageInfo tmp = usages.stream().findAny().get();
        String name = formateTime(tmp.getTime());
        byte[] data = JsonUtils.toJsonBytesQuietly(usages);
        saveObject(diskUsageDirPath + File.separator + name, data);
    }

    @Override
    public Map<String, Collection<GuiNetInfo>> getNetInfos(long time) {
        Collection<GuiNetInfo> array = collectObjects(netInfoDirPath, time, GuiNetInfo[].class);
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
    public void setNetInfos(Collection<GuiNetInfo> netInfos) {
        if (netInfos == null || netInfos.isEmpty()) {
            return;
        }
        GuiNetInfo tmp = netInfos.stream().findAny().get();
        String name = formateTime(tmp.getTime());
        byte[] data = JsonUtils.toJsonBytesQuietly(netInfos);
        saveObject(netInfoDirPath + File.separator + name, data);
    }

    @LifecycleStart
    @Override
    public void start() throws Exception {
        if(!run){
            LOG.info("gui switch is close !!");
            return;
        }else{
            LOG.info("gui starting !!");
        }
        scanPaths = Arrays.asList(
            cpuDirPath,
            memDirPath,
            loadDirPath,
            diskIODirPath,
            diskUsageDirPath,
            netInfoDirPath);
        pool = Executors.newScheduledThreadPool(2, new ThreadFactoryBuilder().setNameFormat("GuiFileWorker").build());
        start = true;
        pool.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                scanPaths.stream().forEach(y -> {
                    if (!start) {
                        LOG.info("stop it ");
                        return;
                    }
                    File file = new File(y);
                    long time = System.currentTimeMillis() - ttlTime;
                    String timeStr = GuiFileMaintainer.this.formateTime(time);
                    Collection<File> loss = GuiFileMaintainer.this.collectTTLFile(file, timeStr);
                    if (loss != null && !loss.isEmpty() && start) {
                        loss.stream().forEach(FileUtils::deleteQuietly);
                    }
                });
            }
        }, 0, intervalTime, TimeUnit.SECONDS);
    }

    @LifecycleStop
    @Override
    public void stop() throws Exception {
        if(!run){
            LOG.info("gui switch is close !!");
            return;
        }else{
            LOG.info("gui stoping !!");
        }
        start = false;
        if (pool != null) {
            pool.shutdown();
        }
    }
}
