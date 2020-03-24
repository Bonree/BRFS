package com.bonree.brfs.identification;

import com.bonree.brfs.common.service.Service;
import com.bonree.brfs.common.utils.BrStringUtils;
import com.bonree.brfs.partition.PartitionInfoRegister;
import com.bonree.brfs.partition.model.LocalPartitionInfo;
import org.apache.commons.lang3.StringUtils;
import org.hyperic.sigar.FileSystem;
import org.hyperic.sigar.Sigar;
import org.hyperic.sigar.SigarException;

import java.io.File;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/*******************************************************************************
 * 版权信息： 北京博睿宏远数据科技股份有限公司
 * Copyright (c) 2007-2020 北京博睿宏远数据科技股份有限公司，Inc. All Rights Reserved.
 * @date 2020年03月24日 15:23:07
 * @author: <a href=mailto:zhucg@bonree.com>朱成岗</a>
 * @description:
 ******************************************************************************/

public class PartitionGather {
    private PartitionInfoRegister register;
    private Service localInfo;
    private Map<String,FileSystem> gatherCache = new ConcurrentHashMap<>();
    public static LocalPartitionInfo createBasInfo(String partitionId,String path){
        return null;

    }
    public static FileSystem gather(String path,Collection<String> excludes) throws SigarException {
        File file = new File(path);
        if(!file.exists()||file.isFile()){
            throw new RuntimeException("Startup failed!! data dir ["+path+"] is not exists or is file !!");
        }
        Sigar sigar = new Sigar();
        try {
            FileSystem[] fss = sigar.getFileSystemList();
            String mountedPoint;
            int type;
            for(FileSystem fileSystem : fss){
                mountedPoint = fileSystem.getDirName();
                type = fileSystem.getType();
                if(BrStringUtils.isEmpty(mountedPoint)){
                    continue;
                }
                fileSystem.gather(sigar);
                // 过滤无关的磁盘
                if(excludes != null && excludes.contains(mountedPoint)){
                    continue;
                }
                //目录无关的分区
                if(StringUtils.indexOf(path,mountedPoint)!=0){
                    continue;
                }
                if(type == 2||type == 3){
                    return fileSystem;
                }
            }
            throw new RuntimeException("Startup failed!! data dir ["+path+"] is not exists or type error !!");
        } finally {
            sigar.close();
        }
    }
}
