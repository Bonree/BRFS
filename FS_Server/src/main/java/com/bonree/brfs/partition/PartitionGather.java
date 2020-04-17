package com.bonree.brfs.partition;

import com.bonree.brfs.common.lifecycle.LifecycleStart;
import com.bonree.brfs.common.lifecycle.LifecycleStop;
import com.bonree.brfs.common.lifecycle.ManageLifecycle;
import com.bonree.brfs.common.process.LifeCycle;
import com.bonree.brfs.common.service.Service;
import com.bonree.brfs.partition.model.LocalPartitionInfo;
import com.bonree.brfs.partition.model.PartitionInfo;
import com.google.inject.Inject;
import org.apache.commons.io.FileUtils;
import org.hyperic.sigar.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.*;

/*******************************************************************************
 * 版权信息： 北京博睿宏远数据科技股份有限公司
 * Copyright (c) 2007-2020 北京博睿宏远数据科技股份有限公司，Inc. All Rights Reserved.
 * @date 2020年03月25日 15:09:15
 * @author: <a href=mailto:zhucg@bonree.com>朱成岗</a>
 * @description:
 ******************************************************************************/
@ManageLifecycle
public class PartitionGather implements LifeCycle {
    private final static Logger LOG = LoggerFactory.getLogger(PartitionGather.class);
    /**
     * 定时执行线程池
     */
    private ScheduledExecutorService pool = null;
    /**
     * 采集线程
     */
    private GatherThread worker = null;

    private int intervalTimes = 5;

    private LocalPartitionListener listener = null;
    public PartitionGather(PartitionInfoRegister register, Service localInfo, Collection<LocalPartitionInfo> validPartions,int intervalTimes) {
        this.intervalTimes = intervalTimes;
        this.pool = Executors.newScheduledThreadPool(1);
        this.worker = new GatherThread(register,validPartions,localInfo);
    }
    @LifecycleStart
    @Override
    public void start() {
        this.worker.setAlive(true);
        this.pool.scheduleAtFixedRate(this.worker,0,this.intervalTimes, TimeUnit.SECONDS);
        LOG.info("partition gather work start");
    }
    @LifecycleStop
    @Override
    public void stop() {
        if(this.worker !=null){
            this.worker.setAlive(false);
        }
        if(this.pool != null){
            this.pool.shutdown();
        }
        LOG.info("partition gather work stop");
    }

    public void setListener(LocalPartitionListener listener) {
        this.listener = listener;
    }

    /**
     * 资源采集线程
     */
    protected static class GatherThread implements Runnable{
        private static final Logger LOG = LoggerFactory.getLogger(GatherThread.class);
        private PartitionInfoRegister register = null;
        private Collection<LocalPartitionInfo> partitions;
        private Sigar sigar = null;
        private Service firstServer;
        private boolean isAlive = true;
        private LocalPartitionListener listener =null;
        public GatherThread(PartitionInfoRegister register,Collection<LocalPartitionInfo> partitions, Service firstServer) {
            this.register = register;
            this.partitions = partitions;
            this.sigar = new Sigar();
            this.firstServer = firstServer;
            this.isAlive = true;
        }

        @Override
        public void run() {
            if(partitions == null || partitions.isEmpty()){
                return;
            }
            LocalPartitionInfo local;
            FileSystem fs;
            FileSystemMap fsMap = null;
            // 获取磁盘信息发生错 则注销所有的磁盘节点信息
            try {
                fsMap = sigar.getFileSystemMap();
            } catch (SigarException e) {
                LOG.error("gather filesystem happen error ",e);
                for(LocalPartitionInfo tmp : partitions){
                    try {
                        register.unregisterPartitionInfo(tmp.getPartitionGroup(),tmp.getPartitionId());
                    } catch (Exception ex) {
                        LOG.error("unregister id happen error !!{}",tmp.getDataDir());
                    }
                }
                return;
            }
            // 正常检查分区是否可用
            PartitionInfo partition;
            for(LocalPartitionInfo elePart : partitions){
                if(!isAlive){
                    break;
                }
                try {
                    fs = fsMap.getMountPoint(elePart.getDataDir());
                    if(PartitionGather.isValid(elePart,fs,sigar)){
                        partition =packagePartition(elePart,fs);
                        register.registerPartitionInfo(partition);
                        if(listener !=null){
                            listener.add(elePart);
                        }
                    }else{
                        register.unregisterPartitionInfo(elePart.getPartitionGroup(),elePart.getPartitionId());
                        if(listener!=null){
                            listener.remove(elePart);
                        }
                    }
                } catch (Exception e) {
                    LOG.error("check partition happen error !!{}",elePart.getDataDir(),e);
                }
            }
            LOG.debug("partition gather work end !!");
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
                FileSystemUsage usage = sigar.getFileSystemUsage(fs.getDirName());
                obj.setFreeSize(usage.getAvail());
            }
            return obj;
        }

        public boolean isAlive() {
            return isAlive;
        }

        public void setAlive(boolean alive) {
            isAlive = alive;
        }

        public LocalPartitionListener getListener() {
            return listener;
        }

        public void setListener(LocalPartitionListener listener) {
            this.listener = listener;
        }
    }
    /**
     * 判断磁盘分区是否有效
     * @param local
     * @return
     */
    public static  boolean isValid(LocalPartitionInfo local, FileSystem fs, Sigar sigar){
        try {
            // fs为空
            if(fs == null){
                return false;
            }

            // 设备名称不一致
            if(!local.getDevName().equals(fs.getDevName())){
                LOG.warn("devName is not same before[{}],after[{}]",local.getDevName(),fs.getDevName());
                return false;
            }
            // 挂载点不一致
            if(!local.getMountPoint().equals(fs.getDirName())){
                LOG.warn("mountPoint is not same before[{}],after[{}]",local.getMountPoint(),fs.getDirName());
                return false;
            }
            // 磁盘分区使用信息为空
            FileSystemUsage usage = sigar.getFileSystemUsage(fs.getDirName());
            if(usage == null){
                return false;
            }
            long value = usage.getTotal();
            // 磁盘分区大小不一致
            if(local.getTotalSize() != value){
                LOG.warn("size is not same before[{}],after[{}]",local.getTotalSize(),value);
                return false;
            }
        } catch (SigarException e) {
            LOG.error("gather{} FileSystemUsage happen ",local.getDevName(),e);
            return false;
        }
        // 读写存在问题
        if(!isValid(local)){
            return false;
        }
        return true;
    }

    /**
     * 判断分区读写是否正常
     * @param local
     * @return
     */
    public static boolean isValid(LocalPartitionInfo local){
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
