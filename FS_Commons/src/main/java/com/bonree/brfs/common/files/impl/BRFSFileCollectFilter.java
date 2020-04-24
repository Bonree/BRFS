package com.bonree.brfs.common.files.impl;

import java.util.Map;

/**
 * 收集BRFS系统存储的文件
 */
public class BRFSFileCollectFilter extends BRFSFileBaseFilter {
    public BRFSFileCollectFilter() {
        super();
    }

    @Override
    public String getKey(int index) {
        return keyMap.get(index);
    }

    @Override
    public boolean isDeep(int index, Map<String, String> values) {
        return index < keyMap.size();
    }

    @Override
    public boolean isAdd(String root, Map<String, String> values, boolean isFile) {
        // 过滤数据根目录
        if (values == null || values.isEmpty()) {
            return false;
        }
        // 过滤目录
        if (!isFile) {
            return false;
        }
        return values.size() == keyMap.size();
    }
}
