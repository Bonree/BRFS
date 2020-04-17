package com.bonree.brfs.tasks.monitor.impl;

import com.bonree.brfs.common.ZookeeperPaths;
import com.bonree.brfs.common.lifecycle.LifecycleStart;
import com.bonree.brfs.common.lifecycle.LifecycleStop;
import com.bonree.brfs.common.lifecycle.ManageLifecycle;
import com.bonree.brfs.common.rebalance.Constants;
import com.bonree.brfs.tasks.monitor.RebalanceTaskMonitor;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.inject.Inject;
import org.apache.curator.framework.CuratorFramework;

import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * 任务终结检查类,定时检查任务状态，
 */
@ManageLifecycle
public class CycleRebalanceTaskMonitor implements RebalanceTaskMonitor {
    private volatile boolean execute = false;
    private CheckTaskThread checkTaskThread;
    private int intervalTime = 5;
    private ScheduledExecutorService pool = null;

    public CycleRebalanceTaskMonitor(CuratorFramework client, String monitorPath, int intervalTime) {
        checkTaskThread = new CheckTaskThread(client,monitorPath+ Constants.SEPARATOR+Constants.TASKS_NODE);
        this.intervalTime = intervalTime;
    }
    @Inject
    public CycleRebalanceTaskMonitor(CuratorFramework client, ZookeeperPaths monitorPath) {
        this(client,monitorPath.getBaseRebalancePath(),1);
    }
    @LifecycleStart
    public void start(){
        if(checkTaskThread !=null){
            checkTaskThread.setBreakFlag(false);
            pool = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryBuilder().setNameFormat("TaskMonitor").build());
            pool.scheduleAtFixedRate(checkTaskThread,0,intervalTime, TimeUnit.SECONDS);
        }
    }
    @LifecycleStop
    public void stop(){
        if(checkTaskThread !=null){
            checkTaskThread.setBreakFlag(true);
        }
        if(pool!=null){
            pool.shutdownNow();
        }
    }

    @Override
    public boolean isExecute() {
        return execute;
    }

    /**
     * 检查副本平衡任务是否存线程
     */
    private class CheckTaskThread implements Runnable{
        private CuratorFramework client;
        private String path = null;
        private boolean breakFlag = false;

        public CheckTaskThread(CuratorFramework client, String path) {
            this.client = client;
            this.path = path;
        }

        public boolean isBreakFlag() {
            return breakFlag;
        }

        public void setBreakFlag(boolean breakFlag) {
            this.breakFlag = breakFlag;
        }

        @Override
        public void run() {
            if(!isBreakFlag()){
                execute = isRecovery(client,path);
            }
        }
        /**
         * 概述：恢复任务是否执行判断
         * @param client
         * @param path
         * @return
         * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
         */
        private boolean isRecovery(CuratorFramework client, String path){
            List<String> paths = null;
            try {
                if(client.checkExists().forPath(path) == null){
                    return false;
                }
                paths = client.getChildren().forPath(path);
                if(paths == null || paths.isEmpty()){
                    return false;
                }
            } catch (Exception e) {
                return false;
            }
            String snPath;
            List<String> cList;
            String tmpPath;
            for(String sn : paths){
                try {
                    if(isBreakFlag()){
                        return false;
                    }
                    snPath = path + Constants.SEPARATOR +sn;
                    if(client.checkExists().forPath(snPath)==null){
                        continue;
                    }
                    cList = client.getChildren().forPath(snPath);
                    if(cList !=null && !cList.isEmpty()){
                        tmpPath = snPath +"/"+ cList.get(0);
                        if(client.checkExists().forPath(tmpPath) == null){
                            continue;
                        }
                        return true;
                    }
                } catch (Exception e) {
                }
            }
            return false;
        }
    }
}
