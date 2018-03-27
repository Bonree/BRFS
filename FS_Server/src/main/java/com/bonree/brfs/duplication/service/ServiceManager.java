package com.bonree.brfs.duplication.service;

import java.util.List;

import com.bonree.brfs.duplication.utils.LifeCycle;

public interface ServiceManager extends LifeCycle {
	void registerService(Service service) throws Exception;
	void unregisterService(Service service) throws Exception;
	void addServiceStateListener(String group, ServiceStateListener listener) throws Exception;
	void removeServiceStateListenerByGroup(String group);
	void removeServiceStateListener(String group, ServiceStateListener listener);
	List<Service> getServiceListByGroup(String serviceGroup);
	Service getServiceById(String group, String serviceId);
}
