package com.bonree.brfs.schedulers;

import static org.junit.Assert.*;

import java.util.Arrays;

import org.junit.Test;
import java.util.List;

public class RightTaskStateWatchTest {

	@Test
	public void testGetPathType() {
		String root = "/data/da";
		String taskName = "/data/da/SYSTEM_DELETE";
		String [] rightArray = new String[]{
				"/data/da/SYSTEM_DELETE",
				"/data/da/SYSTEM_DELETE/1000000001",
				"/data/da/SYSTEM_DELETE/1000000001/1",
				"/data/da/SYSTEM_DELETE/0000000001/1",
				"/data/da/SYSTEM_DELETE/0000000001/1/"
		};
		String [] errorArray = new String[]{
				"/data/da/SYSTEM_DELETE/0000000001/1/123",
				"/data/da/",
				"/data/da/welll",
				"/data/da/welll/",
				"/data/da/welll/123456",
				"/data/da/welll/123456/1",
				"/data"
		};
		RightTaskStateWatch a = new RightTaskStateWatch("ttttt");
		for(String str :errorArray){
			assertEquals("error", -1, a.getPathType(root, str));
		}
		List<Integer> rightCode = (List) Arrays.asList(new Integer[]{ 1,2,3});
		for(String str : rightArray){
			int size = a.getPathType(root, str);
			assertEquals("right test", true, rightCode.contains(size));
		}
	}

}
