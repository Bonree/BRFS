package com.bonree.brfs.zcg;

import com.bonree.brfs.common.zookeeper.curator.CuratorClient;

public class Test1 {
	
	public static void main(String[] args) {
		CuratorClient client = CuratorClient.getClientInstance("192.168.101.86:2181");
		client.setData("/zcg/test", "222".getBytes());
		client.close();
	}

}
