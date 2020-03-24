package com.bonree.brfs.identification;

import com.bonree.brfs.common.process.LifeCycle;
import com.bonree.brfs.common.service.Service;
import com.bonree.brfs.common.utils.BrStringUtils;
import com.bonree.brfs.common.utils.JsonUtils;
import com.bonree.brfs.partition.PartitionInfoRegister;
import com.bonree.brfs.partition.model.LocalPartitionInfo;
import com.bonree.brfs.partition.model.PartitionInfo;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.filefilter.IOFileFilter;
import org.hyperic.sigar.FileSystem;
import org.hyperic.sigar.FileSystemUsage;
import org.hyperic.sigar.Sigar;
import org.hyperic.sigar.SigarException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;

/*******************************************************************************
 * 版权信息： 北京博睿宏远数据科技股份有限公司
 * Copyright (c) 2007-2020 北京博睿宏远数据科技股份有限公司，Inc. All Rights Reserved.
 * @date 2020年03月24日 15:23:07
 * @author: <a href=mailto:zhucg@bonree.com>朱成岗</a>
 * @description:
 ******************************************************************************/

public class PartitionGather implements LifeCycle {
    /**
     * 磁盘分区注册助手
     */
    private PartitionInfoRegister register;
    /**
     * 本机一级server信息
     */
    private Service localInfo;
    /**
     * 本机有效磁盘分区
     */
    private Map<String,FileSystem> gatherCache = new ConcurrentHashMap<>();
    /**
     * 定时执行线程池
     */
    private ExecutorService pool = null;
    /**
     * 采集线程
     */
    private GatherThread worker = null;

    /**
     * 将基础信息有效
     * @param locals
     * @param dataDir
     * @param excludes
     * @return
     * @throws Exception
     */
    public Collection<LocalPartitionInfo> fixValidValue(List<LocalPartitionInfo> locals,Collection<String> dataDir,Collection<String> excludes)throws Exception{
        Map<String,FileSystem> fsMap = init(dataDir,excludes);
        if(fsMap == null || fsMap.isEmpty()){
            throw new RuntimeException("no partition in {}"+dataDir);
        }
        return null;
    }
    /**
     * 获取所有的磁盘分区的挂载点及采集对象 排除不需要的目录
     * @param excludes
     * @return
     * @throws SigarException
     */
    public Map<String,FileSystem> init(Collection<String> dataDir, Collection<String> excludes) throws SigarException {
        Map<String,FileSystem> fsMap= new ConcurrentHashMap<>();
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
                // 过滤无关的磁盘
                if(!isRequird(mountedPoint,dataDir,excludes)){
                    continue;
                }
                if(type == 2||type == 3){
                    fsMap.put(mountedPoint,fileSystem);
                }
            }
        } finally {
            sigar.close();
        }
        return fsMap;
    }

    /**
     * 判断挂载点是否在目录中
     * @param mountedPoint
     * @param dataDir
     * @param excludes
     * @return
     */
    public boolean isRequird(String mountedPoint,Collection<String> dataDir,Collection<String> excludes){
        // 过滤无关的磁盘
        if(excludes != null && excludes.contains(mountedPoint)){
            return false;
        }
        for(String dir : dataDir){
            if(dir.indexOf(mountedPoint) ==0){
                return true;
            }
        }
        return false;
    }

    @Override
    public void start() throws Exception {

    }

    @Override
    public void stop() throws Exception {

    }

    /**
     * 资源采集线程
     */
    private static class GatherThread implements Runnable{
        private static final Logger LOG = LoggerFactory.getLogger(GatherThread.class);
        private PartitionInfoRegister register = null;
        private Map<LocalPartitionInfo,FileSystem> partitionMap =null;
        private Sigar sigar = null;
        private Service firstServer;
        public GatherThread(PartitionInfoRegister register, Map<LocalPartitionInfo, FileSystem> partitionMap,Service firstServer) {
            this.register = register;
            this.partitionMap = partitionMap;
            this.sigar = new Sigar();
            this.firstServer = firstServer;
        }

        @Override
        public void run() {
            if(partitionMap == null || partitionMap.isEmpty()){
                return;
            }
            LocalPartitionInfo local;
            FileSystem fs;
            PartitionInfo partition;

            for(Map.Entry<LocalPartitionInfo,FileSystem> entry : partitionMap.entrySet()){
                local = entry.getKey();
                fs = entry.getValue();
                try {
                    if(isVaild(local,fs)){
                        partition =packagePartition(local,fs);
                        register.registerPartitionInfo(partition);
                    }else{
                        register.unregisterPartitionInfo(local.getPartitionGroup(),local.getPartitionId());
                    }
                } catch (Exception e) {
                    LOG.error("check partition happen error !!{}",local.getDataDir());
                }
            }
        }
        private PartitionInfo packagePartition(LocalPartitionInfo local,FileSystem fs)throws Exception{
            PartitionInfo obj = new PartitionInfo();
            obj.setPartitionGroup(local.getPartitionGroup());
            obj.setPartitionId(local.getPartitionId());
            obj.setServiceGroup(firstServer.getServiceGroup());
            obj.setServiceId(firstServer.getServiceId());
            obj.setRegisterTime(System.currentTimeMillis());
            obj.setTotalSize(local.getTotalSize());
            if(fs !=null){
                FileSystemUsage usage = sigar.getFileSystemUsage(fs.getDevName());
                obj.setFreeSize(usage.getFree()/1024/1024);
            }
            return obj;
        }

        /**
         * 判断磁盘分区是否有效
         * @param local
         * @param fs
         * @return
         */
        private boolean isVaild(LocalPartitionInfo local, FileSystem fs){
            try {
                // fs为空
                if(fs == null){
                    return false;
                }
                // 同步采集信息
                fs.gather(sigar);
                // 设备名称不一致
                if(!local.getDevName().equals(fs.getDevName())){
                    return false;
                }
                // 挂载点不一致
                if(!local.getDataDir().equals(fs.getDirName())){
                    return false;
                }
                // 磁盘分区使用信息为空
                FileSystemUsage usage = sigar.getFileSystemUsage(fs.getDevName());
                if(usage == null){
                    return false;
                }
                // 磁盘分区大小不一致
                if(local.getTotalSize() != usage.getTotal()){
                    return false;
                }
            } catch (SigarException e) {
                LOG.error("gather{} FileSystemUsage happen ",local.getDevName(),e);
                return false;
            }
            // 读写存在问题
            if(!isVaild(local)){
                return false;
            }
            return true;
        }

        /**
         * 判断分区读写是否正常
         * @param local
         * @return
         */
        private boolean isVaild(LocalPartitionInfo local){
            String path = local.getDataDir();
            File testFile = new File(path+"/brfs.test");
            String content = "BRFSTEST";
            try {
                if(testFile.exists()){
                    FileUtils.forceDelete(testFile);
                }
                FileUtils.writeStringToFile(testFile,content, Charset.defaultCharset());
                String tmp = FileUtils.readFileToString(testFile,Charset.defaultCharset());
                return content.equals(tmp);
            } catch (IOException e) {
                LOG.error("read file:[{}] happen error !!",testFile,e);
                return false;
            } finally {
                try {
                    if(testFile.exists()){
                        FileUtils.forceDelete(testFile);
                    }
                } catch (IOException e) {
                    LOG.error("delete file:[{}] happen error !!",testFile,e);
                }
            }
        }
    }
}
