package com.bonree.brfs.duplication;

import java.util.UUID;

public class ServiceIdBuilder {
	
	public static String getServiceId() {
		return UUID.randomUUID().toString();
	}
}
