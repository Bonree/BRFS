package com.bonree.brfs.duplication.storagename;

import java.io.IOException;

import com.bonree.brfs.duplication.utils.ProtoStuffUtils;

public class TestNode {

	public static void main(String[] args) throws IOException {
		StorageNameNode node = new StorageNameNode("sn_1", 123321, 3, 100);
		
		byte[] bytes = ProtoStuffUtils.serialize(node);
		
		StorageNameNode node2 = ProtoStuffUtils.deserialize(ProtoStuffUtils.serialize(node), StorageNameNode.class);
		
		System.out.println("--" + node2);
	}

}
