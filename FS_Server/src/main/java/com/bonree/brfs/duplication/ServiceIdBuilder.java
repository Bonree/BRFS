package com.bonree.brfs.duplication;

import java.util.UUID;

public class ServiceIdBuilder {
	private static final String serviceID = UUID.randomUUID().toString();
	
	public static String getServiceId() {
		return serviceID;
	}
}
