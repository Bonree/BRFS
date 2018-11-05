package com.bonree.brfs.common.files.impl;

import com.bonree.brfs.common.utils.BRFSPath;

import java.util.Map;

public class BRFSTimeFilter extends BRFSFileBaseFilter{

    protected long startTime = 0L;
    protected long endTime = 0L;
    public BRFSTimeFilter(long startTime, long endTime){
        super();
        this.startTime = startTime;
        this.endTime = endTime;
    }
    @Override
    public String getKey(int index){
        return keyMap.get(index);
    }

    @Override
    public boolean isDeep(int index, Map<String, String> values){
        if(index >= keyMap.size()){
            return false;
        }
        if(!values.containsKey(BRFSPath.YEAR)
            ||!values.containsKey(BRFSPath.MONTH)
            ||!values.containsKey(BRFSPath.DAY)
            ||!values.containsKey(BRFSPath.TIME)){
            return true;
        }
        long time = BRFSPath.convertTime(values);
        if(startTime > 0 && time < startTime){
            return false;
        }
        if(endTime >0 && time > endTime){
            return false;
        }
        return true;
    }

    @Override
    public boolean isAdd(String root, Map<String, String> values, boolean isFile){
        if(values.size() != keyMap.size() - 1){
            return false;
        }
        long time = BRFSPath.convertTime(values);
        if(startTime > 0 && time < startTime){
            return false;
        }
        if(endTime >0 && time > endTime){
            return false;
        }
        return true;
    }

}
