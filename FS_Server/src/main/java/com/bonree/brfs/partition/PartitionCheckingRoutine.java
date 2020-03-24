package com.bonree.brfs.partition;

import com.bonree.brfs.common.utils.JsonUtils;
import com.bonree.brfs.partition.model.LocalPartitionInfo;
import com.bonree.brfs.server.identification.LevelServerIDGen;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


/*******************************************************************************
 * 版权信息： 北京博睿宏远数据科技股份有限公司
 * Copyright (c) 2007-2020 北京博睿宏远数据科技股份有限公司，Inc. All Rights Reserved.
 * @date 2020年03月24日 20:45:10
 * @author: <a href=mailto:zhucg@bonree.com>朱成岗</a>
 * @description: 磁盘分区校验程序，负责对比配置文件和内部文件的区别，当有新的路径 则申请新的磁盘分区id，若存在旧的磁盘分区被去掉，则创建磁盘变更
 ******************************************************************************/

public class PartitionCheckingRoutine {
    private LevelServerIDGen idGen;
    // todo 磁盘变更主动发布接口
    //private xxx
    private String dataConfig;
    private String innerDir;

    public PartitionCheckingRoutine(LevelServerIDGen idGen, String dataConfig, String innerDir) {
        this.idGen = idGen;
        this.dataConfig = dataConfig;
        this.innerDir = innerDir;
    }
    public Collection<LocalPartitionInfo> checkVaildPartition(){
        String[] dirs = StringUtils.split(dataConfig,",");
        // todo 若无数据目录则 抛出异常，
        if(dirs == null || dirs.length ==0){

        }
        // 获取有效的磁盘分区

        // 判断是否存在多个目录位于一个磁盘分区的情况，若存在则抛异常

        // 判断是否存在增加的磁盘分区，若存在则申请磁盘id

        // 判断是否存在减少的磁盘分区，若存在则创建磁盘变更信息

        // 返回有效的磁盘分区，该数据将提供给定时线程做定时上报处理

        return null;
    }

    /**
     * 获取本地已经注册的id文件
     * @param path
     * @return
     */
    public Map<String, LocalPartitionInfo> readIds(String path){
        File file = new File(path);
        if(!file.exists()){
            throw new RuntimeException("path ["+path+"] is not exists !!");
        }
        if(!file.isDirectory()){
            throw new RuntimeException("path ["+path+"] is not directory !!");
        }
        File[] files = file.listFiles();
        if(files == null || files.length == 0){
            return null;
        }
        Map<String,LocalPartitionInfo> map = new HashMap<>(files.length);
        LocalPartitionInfo part;
        for(File f :files){
            part = readByFile(f);
            map.put(part.getDataDir(),part);
        }
        return map;
    }

    /**
     * 读取内部id文件
     * @param file
     * @return
     */
    private LocalPartitionInfo readByFile(File file){
        if(!file.isFile()){
            throw new RuntimeException("path ["+file.getAbsolutePath()+"] is not file !!");
        }
        try {
            byte[] data  = FileUtils.readFileToByteArray(file);
            if(data == null || data.length==0){
                throw new RuntimeException("path ["+file.getAbsolutePath()+"] is empty !!");
            }
            LocalPartitionInfo partitionInfo = JsonUtils.toObject(data,LocalPartitionInfo.class);
            return partitionInfo;
        } catch (IOException e) {
            throw new RuntimeException("path ["+file.getAbsolutePath()+"] read happen error !!",e);
        } catch (JsonUtils.JsonException e) {
            throw new RuntimeException("path ["+file.getAbsolutePath()+"] convert to instance happen error !!",e);
        }
    }
}
