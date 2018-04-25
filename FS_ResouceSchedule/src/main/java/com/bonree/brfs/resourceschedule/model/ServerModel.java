package com.bonree.brfs.resourceschedule.model;

public class ServerModel {
	private BaseMetaServerModel base = null;
	private ResourceModel resource = null;
	public ServerModel(){
		
	}
	public BaseMetaServerModel getBase() {
		return base;
	}
	public void setBase(BaseMetaServerModel base) {
		this.base = base;
	}
	public ResourceModel getResource() {
		return resource;
	}
	public void setResource(ResourceModel resource) {
		this.resource = resource;
	}
	
}
