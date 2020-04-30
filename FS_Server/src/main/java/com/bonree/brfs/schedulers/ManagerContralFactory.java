package com.bonree.brfs.schedulers;

import com.bonree.brfs.common.ZookeeperPaths;
import com.bonree.brfs.common.service.ServiceManager;
import com.bonree.brfs.common.task.TaskType;
import com.bonree.brfs.common.zookeeper.curator.CuratorClient;
import com.bonree.brfs.duplication.storageregion.StorageRegionManager;
import com.bonree.brfs.identification.IDSManager;
import com.bonree.brfs.identification.impl.DiskDaemon;
import com.bonree.brfs.rebalance.route.RouteLoader;
import com.bonree.brfs.resource.vo.LimitServerResource;
import com.bonree.brfs.schedulers.task.manager.MetaTaskManagerInterface;
import com.bonree.brfs.schedulers.task.manager.RunnableTaskInterface;
import com.bonree.brfs.schedulers.task.manager.SchedulerManagerInterface;
import com.bonree.brfs.tasks.monitor.RebalanceTaskMonitor;
import java.util.ArrayList;
import java.util.List;

public class ManagerContralFactory {
    /**
     * serverId管理服务
     */
    private ServiceManager sm;
    /**
     * storageName管理服务
     */
    private StorageRegionManager snm;
    /**
     * 任务发布服务
     */
    private MetaTaskManagerInterface tm;
    /**
     * 任务执行服务
     */
    private SchedulerManagerInterface stm;
    private RunnableTaskInterface rt;
    private IDSManager sim;
    /**
     * 开启任务列表
     */
    private List<TaskType> taskOn = new ArrayList<TaskType>();
    private ZookeeperPaths zkPath = null;
    private CuratorClient client = null;
    private LimitServerResource limitServerResource;
    // TODO: 4/14/20 没有赋值操作
    private RouteLoader routeLoader;
    // TODO: 4/14/20 没有赋值操作
    private DiskDaemon daemon;
    // TODO: 4/14/20 没有赋值操作
    private RebalanceTaskMonitor taskMonitor;

    String serverId;
    String groupName;

    private ManagerContralFactory() {
    }

    private static class SimpleInstance {
        public static ManagerContralFactory instance = new ManagerContralFactory();
    }

    public static ManagerContralFactory getInstance() {
        return SimpleInstance.instance;
    }

    public boolean isEmpty() {
        if (this.sm == null || this.snm == null) {
            return true;
        }
        return false;
    }

    public ServiceManager getSm() {
        return sm;
    }

    public void setSm(ServiceManager sm) {
        this.sm = sm;
    }

    public StorageRegionManager getSnm() {
        return snm;
    }

    public void setSnm(StorageRegionManager snm) {
        this.snm = snm;
    }

    public MetaTaskManagerInterface getTm() {
        return tm;
    }

    public void setTm(MetaTaskManagerInterface tm) {
        this.tm = tm;
    }

    public SchedulerManagerInterface getStm() {
        return stm;
    }

    public void setStm(SchedulerManagerInterface stm) {
        this.stm = stm;
    }

    public String getServerId() {
        return serverId;
    }

    public void setServerId(String serverId) {
        this.serverId = serverId;
    }

    public String getGroupName() {
        return groupName;
    }

    public void setGroupName(String groupName) {
        this.groupName = groupName;
    }

    public List<TaskType> getTaskOn() {
        return taskOn;
    }

    public void setTaskOn(List<TaskType> taskOn) {
        this.taskOn = taskOn;
    }

    public RunnableTaskInterface getRt() {
        return rt;
    }

    public void setRt(RunnableTaskInterface rt) {
        this.rt = rt;
    }

    public IDSManager getSim() {
        return sim;
    }

    public void setSim(IDSManager sim) {
        this.sim = sim;
    }

    public ZookeeperPaths getZkPath() {
        return zkPath;
    }

    public void setZkPath(ZookeeperPaths zkPath) {
        this.zkPath = zkPath;
    }

    public CuratorClient getClient() {
        return client;
    }

    public void setClient(CuratorClient client) {
        this.client = client;
    }

    public LimitServerResource getLimitServerResource() {
        return limitServerResource;
    }

    public void setLimitServerResource(LimitServerResource limitServerResource) {
        this.limitServerResource = limitServerResource;
    }

    public RouteLoader getRouteLoader() {
        return routeLoader;
    }

    public void setRouteLoader(RouteLoader routeLoader) {
        this.routeLoader = routeLoader;
    }

    public DiskDaemon getDaemon() {
        return daemon;
    }

    public void setDaemon(DiskDaemon daemon) {
        this.daemon = daemon;
    }

    public RebalanceTaskMonitor getTaskMonitor() {
        return taskMonitor;
    }

    public void setTaskMonitor(RebalanceTaskMonitor taskMonitor) {
        this.taskMonitor = taskMonitor;
    }
}
