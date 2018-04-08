package com.bonree.brfs.resourceschedule.utils;

import static org.junit.Assert.*;

import org.junit.Test;

import com.bonree.brfs.resourceschedule.commons.GatherResource;

public class NetUtilsTest {

	@Test
	public void test() {
		String[] errorIps = {null,
				"",
				"0.0.0.0",
				"1234567890123456",
				"255.255.255.256",
				"127.0.0.1",
				"11.11",
				"255. 255.255.255",
				"255. 0 . 00.1",
				"127.1.1. 0",
				"a.12.12.12",
				"--.00.00.00",
				"00.00.00.00",
				"012.01.01.02"
				};
		for(String ip : errorIps){
			assertEquals(ip,true, NetUtils.filterIp(ip));
		}
	}

}
