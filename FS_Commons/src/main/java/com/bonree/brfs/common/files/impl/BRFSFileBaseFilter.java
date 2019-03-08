package com.bonree.brfs.common.files.impl;

import com.bonree.brfs.common.files.FileFilterInterface;
import com.bonree.brfs.common.utils.BRFSPath;

import java.util.HashMap;
import java.util.Map;

public abstract class BRFSFileBaseFilter implements FileFilterInterface{
    protected Map<Integer, String> keyMap = new HashMap<>();
    public BRFSFileBaseFilter(){
        keyMap.put(0, BRFSPath.STORAGEREGION);
        keyMap.put(1,BRFSPath.INDEX);
        keyMap.put(2,BRFSPath.YEAR);
        keyMap.put(3,BRFSPath.MONTH);
        keyMap.put(4,BRFSPath.DAY);
        keyMap.put(5,BRFSPath.TIME);
        keyMap.put(6,BRFSPath.FILE);
    }
}
