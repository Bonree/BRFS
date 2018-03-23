package com.bonree.brfs.resourceschedule.utils;

import static org.junit.Assert.*;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.junit.Test;

import com.bonree.brfs.resourceschedule.commons.impl.GatherResource;

public class DiskUtilsTest {

	@Test
	public void test() {
		String[] errorFile = {
				null,
				"",
				"  ",
				"_",
				"2343241234*&^%$$#"
			};
			for(String file : errorFile){
				assertEquals(file, true,DiskUtils.filterMountPoint(file));
			}
	}
	@Test
	public void testIsPartOfPatition(){
		Set<String> mountFiles = new HashSet<String>(Arrays.asList(new String[]{
				"C:\\",
				"C:\\data\\",
				"C:\\data\\file1"
		}));
//		for(String tmp : mountFiles){
//			tmp = tmp.replace("/", File.separator).replace("\\", File.separator);
//		}
		String dir = "C:/tmp";
		assertEquals(dir, "C:\\", DiskUtils.selectPartOfDisk(dir, mountFiles));
		
	}

}
