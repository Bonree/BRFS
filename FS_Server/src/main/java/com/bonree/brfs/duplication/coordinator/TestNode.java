package com.bonree.brfs.duplication.coordinator;

import java.io.IOException;

import com.alibaba.fastjson.JSON;
import com.bonree.brfs.duplication.utils.JsonUtils;
import com.bonree.brfs.duplication.utils.ProtoStuffUtils;

public class TestNode {

	public static void main(String[] args) throws IOException {
		FileNode node = new FileNode();
		node.setName("file_node_1");
		node.setStorageName("sn_1");
		node.setServiceId("me");
		node.setDuplicates(new int[]{1 , 2, 3});
		
		String js = JsonUtils.toJsonString(node);
		System.out.println(js);
		System.out.println(js.getBytes().length);
		
		FileNode node2 = JsonUtils.toObject(js, FileNode.class);
		System.out.println(node2.getName());
		
		
		byte[] bytes = JsonUtils.toJsonBytes(node);
		System.out.println("byte length=" + bytes.length);
		FileNode node3 = JsonUtils.toObject(bytes, FileNode.class);
		System.out.println(node3.getName());
		
		
		System.out.println(ProtoStuffUtils.serialize(node).length);
	}

}
