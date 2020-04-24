package com.bonree.brfs.common.service;

import com.bonree.brfs.common.process.LifeCycle;
import java.util.List;

/**
 * 服务管理接口
 *
 * @author chen
 */
public interface ServiceManager extends LifeCycle {
    /**
     * 服务注册
     *
     * @param service
     *
     * @throws Exception
     */
    void registerService(Service service) throws Exception;

    /**
     * 服务注销
     *
     * @param service
     *
     * @throws Exception
     */
    void unregisterService(Service service) throws Exception;

    /**
     * 更新服务payload信息
     *
     * @param service
     *
     * @throws Exception
     */
    void updateService(String group, String serviceId, String payload) throws Exception;

    /**
     * 添加服务状态监听接口
     *
     * @param group    监听的服务组名
     * @param listener
     *
     * @throws Exception
     */
    void addServiceStateListener(String group, ServiceStateListener listener) throws Exception;

    /**
     * 移除监听指定服务组的所有监听接口
     *
     * @param group
     */
    void removeServiceStateListenerByGroup(String group);

    /**
     * 移除监听指定服务组的某个特定接口
     *
     * @param group
     * @param listener
     */
    void removeServiceStateListener(String group, ServiceStateListener listener);

    /**
     * 获取指定服务组中的所有Service
     *
     * @param serviceGroup
     *
     * @return
     */
    List<Service> getServiceListByGroup(String serviceGroup);

    /**
     * 获取某个服务组中的特定服务
     *
     * @param group
     * @param serviceId
     *
     * @return
     */
    Service getServiceById(String group, String serviceId);
}
