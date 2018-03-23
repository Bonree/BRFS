package com.bonree.brfs.resourceschedule.model;

import static org.junit.Assert.*;

import org.junit.Test;

public class NetStatModelTest {

	@Test
	public void test() {
		NetStatModel net1 = new NetStatModel();
		net1.setIpAddress("192.168.1.101");
		net1.setrDataSize(1000);
		net1.settDataSize(1000);
		NetStatModel net2 = new NetStatModel();
		net2.setIpAddress("192.168.1.101");
		net2.setrDataSize(2000);
		net2.settDataSize(2000);
		NetStatModel net3 = new NetStatModel();
		net3.setIpAddress("192.168.1.101");
		net3.setrDataSize(4000);
		net3.settDataSize(4000);
		NetStatModel net4 = net2.calc(net1);
		NetStatModel net5 = net3.calc(net2);
		NetStatModel net6 = net5.sum(net4);
		System.out.println("net 4 :" + net4);
		System.out.println("net 5 :" + net5);
		System.out.println("net 6 :" + net6);
	}

}
