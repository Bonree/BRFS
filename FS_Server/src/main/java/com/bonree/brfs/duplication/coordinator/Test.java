package com.bonree.brfs.duplication.coordinator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class Test {

	public static void main(String[] args) {
		DuplicateNode node1 = new DuplicateNode();
		node1.setGroup("group1");
		node1.setId("id1");
		
		DuplicateNode node2 = new DuplicateNode();
		node2.setGroup("group2");
		node2.setId("id2");
		
		System.out.println("1-" + node1.hashCode());
		System.out.println("2-" + node2.hashCode());
		
		HashMap<DuplicateNode, String> m = new HashMap<DuplicateNode, String>();
		m.put(node1, "--1");
		m.put(node2, "--2");
		
		System.out.println("##" + m.get(node1));
		System.out.println("##" + m.get(node2));
		
		String[] ss = new String[3];
		for(String s : ss) {
			System.out.println("--" + s);
		}
		
		List<String> ls = new ArrayList<String>(10);
		ls.add("1");
		ls.add("2");
		System.out.println("--" + ls.size());
	}

}
