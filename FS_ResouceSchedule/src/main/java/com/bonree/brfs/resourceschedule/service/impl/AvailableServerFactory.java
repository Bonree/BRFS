package com.bonree.brfs.resourceschedule.service.impl;

import com.bonree.brfs.common.utils.BrStringUtils;
import com.bonree.brfs.resourceschedule.service.AvailableServerInterface;

public class AvailableServerFactory {
	public static AvailableServerInterface getAvailableServerInstance(String clazzName){
		AvailableServerInterface interfaceInstance = null;
		if(BrStringUtils.isEmpty(clazzName)){
			return interfaceInstance;
		}
		if(RandomAvailable.class.getName().equals(clazzName)){
			interfaceInstance = RandomAvailable.getInstance();
		}
		return interfaceInstance;
	}
}
