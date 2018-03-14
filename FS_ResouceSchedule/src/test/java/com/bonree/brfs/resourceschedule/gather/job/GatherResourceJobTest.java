package com.bonree.brfs.resourceschedule.gather.job;

import static org.junit.Assert.*;

import java.io.File;

import org.junit.Test;

import com.bonree.brfs.resouceschedule.commons.Commons;
import com.bonree.brfs.resouceschedule.commons.CommonsTest;

public class GatherResourceJobTest {
	@Test
	public void testGatherBaseServerInfo() {
		try {
			if(!CommonsTest.initLibrary()){
				System.out.println("test skip gatherBaseServerInfo");
				return;
			}
			long startTime = System.currentTimeMillis();
			System.out.println(Commons.gatherBaseServerInfo(0, "E:/"));
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
			if (!CommonsTest.initLibrary()) {
				System.out.println("test skip gatherServerStatInfo");
				return;
			}
			long startTime = System.currentTimeMillis();
			System.out.println(Commons.gatherServerStatInfo("E:/"));
			long stopTime = System.currentTimeMillis();
			System.out.println("gather stat info time : " + (stopTime - startTime) + " ms");
		}
		catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
