package com.bonree.brfs.common.files.impl;

import com.bonree.brfs.common.utils.BRFSPath;
import com.bonree.brfs.common.utils.BrStringUtils;

import java.util.Map;

public class BRFSDogFoodFilter extends BRFSFileBaseFilter{
    public BRFSDogFoodFilter(){
        super();
    }

    @Override
    public String getKey(int index){
        return keyMap.get(index);
    }

    @Override
    public boolean isDeep(int index, Map<String, String> values){
        return !isBug(values, false);
    }

    @Override
    public boolean isAdd(String root, Map<String, String> values, boolean isFile){

        return isBug(values, isFile);
    }

    public boolean isBug(Map<String, String> values, boolean isFile){
        if(values == null || values.isEmpty()) {
            return false;
        }
        int vSize = values.size();
        // 超过目录结构的为非法文件
        if(vSize != keyMap.size() && isFile) {
            return true;
        }
        // 在目录节点存在的文件被删除
        if(vSize == keyMap.size() && !isFile) {
            return true;
        }
        String key = null;
        String value = null;
        key = keyMap.get(vSize - 1);
        if(key == null || key.trim().isEmpty()) {
            return true;
        }
        value = values.get(key);
        return !islaw(key, value, isFile);

    }

    public boolean islaw(String key, String value, boolean isFile){
        if(value == null || value.trim().isEmpty()) {
            return false;
        }
        if(BRFSPath.STORAGEREGION.equals(key)) {
            return !isFile;
        }
        if(BRFSPath.INDEX.equals(key) || BRFSPath.YEAR.equals(key) || BRFSPath.MONTH.equals(key) || BRFSPath.DAY.equals(key)) {
            return isFile ? false : BrStringUtils.isNumeric(value);
        }
        if(BRFSPath.TIME.equals(key)) {
            if(isFile) {
                return false;
            }
            if(!value.contains("_")) {
                return false;
            }
            int length = BrStringUtils.getSplit(value, "_").length;
            return length == 3;
        }
        if(BRFSPath.FILE.equals(key)) {
            if(!isFile) {
                return false;
            }
            return value.contains("_");
        }
        return false;
    }
}
