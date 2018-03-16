package com.bonree.brfs.resourceschedule.gather.job;

import static org.junit.Assert.*;

import java.io.File;

import org.junit.Test;

import com.bonree.brfs.resourceschedule.commons.GatherResource;
import com.bonree.brfs.resourceschedule.utils.LibUtilsTest;

public class GatherResourceJobTest {
	@Test
	public void testGatherBaseServerInfo() {
		try {
			if(!LibUtilsTest.initLibrary()){
				System.out.println("test skip gatherBaseServerInfo");
				return;
			}
			long startTime = System.currentTimeMillis();
			System.out.println(GatherResource.gatherBaseServerInfo(0, "E:/"));
			long stopTime = System.currentTimeMillis();
			System.out.println("gather base info time : " + (stopTime - startTime) + " ms");
		}
		catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	@Test
	public void testGatherServerStatInfo() {
		try {
			if (!LibUtilsTest.initLibrary()) {
				System.out.println("test skip gatherServerStatInfo");
				return;
			}
			long startTime = System.currentTimeMillis();
			System.out.println(GatherResource.gatherServerStatInfo("E:/"));
			long stopTime = System.currentTimeMillis();
			System.out.println("gather stat info time : " + (stopTime - startTime) + " ms");
		}
		catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
