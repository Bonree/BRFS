package com.bonree.brfs.schedulers.utils;

import com.bonree.brfs.common.files.impl.BRFSFileBaseFilter;
import com.bonree.brfs.common.utils.BRFSFileUtil;
import com.bonree.brfs.common.utils.BRFSPath;
import com.bonree.brfs.common.utils.BrStringUtils;
import com.bonree.brfs.duplication.storageregion.StorageRegion;
import com.bonree.brfs.rebalance.route.BlockAnalyzer;
import com.bonree.brfs.rebalance.route.impl.RouteParser;
import java.io.File;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class InvaildFileBlockFilter extends BRFSFileBaseFilter {
    private static final Logger LOG = LoggerFactory.getLogger(InvaildFileBlockFilter.class);
    private BlockAnalyzer parser;
    private StorageRegion region;
    private long lastTime;
    private String secondId;

    public InvaildFileBlockFilter(BlockAnalyzer parser, StorageRegion storageRegion, String secondId, long lastTime) {
        super();
        this.parser = parser;
        this.region = storageRegion;
        this.lastTime = lastTime;
        this.secondId = secondId;
    }

    @Override
    public String getKey(int index) {
        return keyMap.get(index);
    }

    @Override
    public boolean isDeep(int index, Map<String, String> values) {
        if (isBug(values, false)) {
            return false;
        }
        if (values.containsKey(BRFSPath.STORAGEREGION)) {
            String tmpStorageName = values.get(BRFSPath.STORAGEREGION);
            if (tmpStorageName == null || tmpStorageName.trim().isEmpty()) {
                return false;
            }
            if (!region.getName().equals(tmpStorageName)) {
                return false;
            }
        }
        if (values.size() == keyMap.size() - 1) {
            long tmp = BRFSPath.convertTime(values);
            return tmp < lastTime;
        }
        return true;
    }

    @Override
    public boolean isAdd(String root, Map<String, String> values, boolean isFile) {

        if (isBug(values, isFile)) {
            LOG.warn("file: [{}]-[{}] is dog food !!", values, isFile);
            return true;
        }
        if (values.size() != keyMap.size()) {
            return false;
        }
        //判断是否为本机该存在的
        String tmpRegion = values.get(BRFSPath.STORAGEREGION);
        if (!region.getName().equals(tmpRegion)) {
            LOG.warn("file region is not match expect {} local", region.getName(), tmpRegion);
            return false;
        }
        String fileName = values.get(BRFSPath.FILE);
        if (fileName.contains(".rd")) {
            LOG.debug("file: [{}]-[{}] contain .rd !!", values, isFile);
            return false;
        }
        if (fileName.contains(".")) {
            LOG.warn("file: [{}]-[{}] contain dot !!", values, isFile);
            return true;
        }
        int index = getIndex(secondId, this.parser, fileName);
        int local = Integer.parseInt(values.get(BRFSPath.INDEX));
        if (index == -1) {
            LOG.warn("file : {} should not in dir {} ", fileName, local);
            return true;
        }
        if (local != index + 1) {
            LOG.warn("file : {} analysis in {} but in {} ", fileName, index + 1, local);
            return true;
        }
        return false;
    }

    private int getIndex(String sid, BlockAnalyzer parser, String fileName) {
        try {
            String[] alives = parser.searchVaildIds(fileName);
            if (alives == null || alives.length == 0) {
                LOG.warn("[{}] analys service error !! alives is null !!!", fileName);
                return -1;
            }
            return Arrays.asList(alives).indexOf(sid);

        } catch (Exception e) {
            LOG.error("check storageregion :{}, file {} happener error", fileName, sid, e);
        }
        return -1;
    }

    private boolean isBug(Map<String, String> values, boolean isFile) {
        if (values == null || values.isEmpty()) {
            return false;
        }
        int size = values.size();
        // 超过目录结构的为非法文件
        if (size != keyMap.size() && isFile) {
            return true;
        }
        // 在目录节点存在的文件被删除
        if (size == keyMap.size() && !isFile) {
            return true;
        }
        String key = null;
        String value = null;
        key = keyMap.get(size - 1);
        if (key == null || key.trim().isEmpty()) {
            return true;
        }
        value = values.get(key);
        return !islaw(key, value, isFile);
    }

    private boolean islaw(String key, String value, boolean isFile) {
        if (value == null || value.trim().isEmpty()) {
            return false;
        }
        if (BRFSPath.STORAGEREGION.equals(key)) {
            return !isFile;
        }
        if (BRFSPath.INDEX.equals(key) || BRFSPath.YEAR.equals(key) || BRFSPath.MONTH.equals(key) || BRFSPath.DAY.equals(key)) {
            return isFile ? false : BrStringUtils.isNumeric(value);
        }
        if (BRFSPath.TIME.equals(key)) {
            if (isFile) {
                return false;
            }
            if (!value.contains("_")) {
                return false;
            }
            int length = BrStringUtils.getSplit(value, "_").length;
            return length == 3;
        }
        if (BRFSPath.FILE.equals(key)) {
            if (!isFile) {
                return false;
            }
            return value.contains("_");
        }
        return false;
    }

}
