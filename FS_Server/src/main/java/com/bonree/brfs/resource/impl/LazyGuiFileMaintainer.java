package com.bonree.brfs.resource.impl;

import com.bonree.brfs.common.utils.JsonUtils;
import com.bonree.brfs.common.utils.TimeUtils;
import com.bonree.brfs.resource.GuiResourceMaintainer;
import com.bonree.brfs.resource.vo.GuiCpuInfo;
import com.bonree.brfs.resource.vo.GuiDiskIOInfo;
import com.bonree.brfs.resource.vo.GuiDiskUsageInfo;
import com.bonree.brfs.resource.vo.GuiLoadInfo;
import com.bonree.brfs.resource.vo.GuiMemInfo;
import com.bonree.brfs.resource.vo.GuiNetInfo;
import com.bonree.brfs.resource.vo.GuiNodeInfo;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.IOFileFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LazyGuiFileMaintainer implements GuiResourceMaintainer {
    private static final Logger LOG = LoggerFactory.getLogger(LazyGuiFileMaintainer.class);
    private String basePath;
    private String nodeFilePath = basePath + File.separator + "sys/node";
    private String cpuDirPath = basePath + File.separator + "cpu";
    private String memDirPath = basePath + File.separator + "mem";
    private String loadDirPath = basePath + File.separator + "load";
    private String diskIODirPath = basePath + File.separator + "disk/io";
    private String diskUsageDirPath = basePath + File.separator + "disk/usage";

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

    private void saveObject(String path,byte[] data){
        if(data == null ||data.length == 0){
            return;
        }
        File file = new File(path);
        try {
            FileUtils.writeByteArrayToFile(file,data,false);
        } catch (IOException e) {
            LOG.error("save happen error content: {}",new String(data),e);
        }
    }
    /**
     * 读取文件
     * @param file
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
                    return belongTime.compareTo(file.getName()) >= 0;
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
                    return ttlTime.compareTo(file.getName()) < 0;
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
       return collectObject(cpuDirPath,time,GuiCpuInfo.class);
    }

    @Override
    public void setCpuInfo(GuiCpuInfo cpuInfo) {
        if(cpuInfo == null){
            return;
        }
        String timeStamp = formateTime(cpuInfo.getTime());
        byte[] data = JsonUtils.toJsonBytesQuietly(cpuInfo);
        saveObject(cpuDirPath+File.separator+timeStamp,data);
    }

    @Override
    public Collection<GuiMemInfo> getMemInfos(long time) {
        return collectObject(cpuDirPath,time,GuiMemInfo.class);
    }

    @Override
    public void setMemInfo(GuiMemInfo memInfo) {
        if(memInfo == null){
            return;
        }
        String timeStamp = formateTime(memInfo.getTime());
        byte[] data = JsonUtils.toJsonBytesQuietly(memInfo);
        saveObject(cpuDirPath+File.separator+timeStamp,data);
    }

    @Override
    public Collection<GuiLoadInfo> getLoadInfos(long time) {
        return collectObject(cpuDirPath,time,GuiLoadInfo.class);
    }

    @Override
    public void setLoadInfo(GuiLoadInfo loadInfo) {
        if(loadInfo == null){
            return;
        }
        String timeStamp = formateTime(loadInfo.getTime());
        byte[] data = JsonUtils.toJsonBytesQuietly(loadInfo);
        saveObject(cpuDirPath+File.separator+timeStamp,data);
    }

    @Override
    public Map<String, Collection<GuiDiskIOInfo>> getDiskIOInfos(long time) {
        return null;
    }

    @Override
    public void setDiskIOs(Collection<GuiDiskIOInfo> iOs) {

    }

    @Override
    public Map<String, Collection<GuiDiskUsageInfo>> getDiskUsages(long time) {
        return null;
    }

    @Override
    public void setDiskUsages(Collection<GuiDiskUsageInfo> usages) {

    }

    @Override
    public Map<String, Collection<GuiNetInfo>> getNetInfos(long time) {
        return null;
    }

    @Override
    public void setNetInfos(Collection<GuiNetInfo> netInfos) {

    }
}
