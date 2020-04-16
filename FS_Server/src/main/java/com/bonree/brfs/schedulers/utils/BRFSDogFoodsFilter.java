package com.bonree.brfs.schedulers.utils;

import com.bonree.brfs.common.files.impl.BRFSDogFoodFilter;
import com.bonree.brfs.common.utils.BRFSFileUtil;
import com.bonree.brfs.common.utils.BRFSPath;
import com.bonree.brfs.duplication.storageregion.StorageRegion;
import com.bonree.brfs.identification.SecondIdsInterface;
import com.bonree.brfs.rebalance.route.impl.RouteParser;
import com.bonree.brfs.server.identification.ServerIDManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class BRFSDogFoodsFilter extends BRFSDogFoodFilter{
    private static final Logger LOG = LoggerFactory.getLogger(BRFSDogFoodsFilter.class);
    private RouteParser parser;
    private StorageRegion  region;
    private long lastTime;
    private String secondId;

    public BRFSDogFoodsFilter(RouteParser parser, StorageRegion storageRegion, String secondId,long lastTime){
        super();
        this.parser = parser;
        this.region = storageRegion;
        this.lastTime = lastTime;
        this.secondId =secondId;
    }
    @Override
    public String getKey(int index){
        return keyMap.get(index);
    }

    @Override
    public boolean isDeep(int index, Map<String, String> values){
        if(isBug(values, false)){
           return false;
        }
        if(values.containsKey(BRFSPath.STORAGEREGION)){
            String tmpStorageName = values.get(BRFSPath.STORAGEREGION);
            if(tmpStorageName == null || tmpStorageName.trim().isEmpty()){
                return false;
            }
            if(!region.getName().equals(tmpStorageName)){
                return false;
            }
        }
        if(values.size() == keyMap.size() -1){
            long tmp = BRFSPath.convertTime(values);
            return tmp < lastTime;
        }
        return true;
    }

    @Override
    public boolean isAdd(String root, Map<String, String> values, boolean isFile){

        if(isBug(values, isFile)){
            LOG.warn("file: [{}]-[{}] is dog food !!",values,isFile);
            return true;
        }
        if(values.size() != keyMap.size()){
            return false;
        }
        //判断是否为本机该存在的
        String tmpRegion = values.get(BRFSPath.STORAGEREGION);
        if(!region.getName().equals(tmpRegion)){
            return false;
        }
        String fileName = values.get(BRFSPath.FILE);
        if(fileName.contains(".rd")){
            LOG.debug("file: [{}]-[{}] contain .rd !!",values,isFile);
            return false;
        }
        if(fileName.contains(".")){
            LOG.warn("file: [{}]-[{}] contain dot !!",values,isFile);
            return true;
        }
        boolean ulawFlag = CopyCountCheck.isUnlaw(secondId,this.parser,fileName);
        if(ulawFlag){
            String path = BRFSFileUtil.createPath(root,values);
            File file = new File(path+".rd");
            return !file.exists();
        }
        return false;
    }

}
