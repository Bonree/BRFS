package com.bonree.brfs.resourceschedule.utils;

import static org.junit.Assert.*;

import org.junit.Test;

import com.bonree.brfs.resourceschedule.commons.GatherResource;

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

}
