package com.bonree.brfs.duplication.service;


public class Service {

	private String serviceId;
	private String serviceGroup;
	private String host;
	private int port;
	
	public Service() {
	}
	
	public Service(String serviceId, String serviceGroup, String host, int port) {
		this.serviceId = serviceId;
		this.serviceGroup = serviceGroup;
		this.host = host;
		this.port = port;
	}

	public String getServiceId() {
		return serviceId;
	}

	public void setServiceId(String serviceId) {
		this.serviceId = serviceId;
	}

	public String getServiceGroup() {
		return serviceGroup;
	}

	public void setServiceGroup(String serviceGroup) {
		this.serviceGroup = serviceGroup;
	}

	public String getHost() {
		return host;
	}

	public void setHost(String host) {
		this.host = host;
	}

	public int getPort() {
		return port;
	}

	public void setPort(int port) {
		this.port = port;
	}

	@Override
	public boolean equals(Object obj) {
		if(this == obj) {
			return true;
		}
		
		if(obj instanceof Service) {
			Service cmp = (Service) obj;
			if(this.serviceId.equals(cmp.serviceId)
					&& this.serviceGroup.equals(cmp.serviceGroup)
					&& this.host.equals(cmp.host)
					&& this.port == cmp.port) {
				return true;
			}
		}
		
		return false;
	}

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("Service[")
		.append("id=").append(serviceId)
		.append(",group=").append(serviceGroup)
		.append(",host=").append(host)
		.append(",port=").append(port)
		.append("]");
		
		return builder.toString();
	}
}
