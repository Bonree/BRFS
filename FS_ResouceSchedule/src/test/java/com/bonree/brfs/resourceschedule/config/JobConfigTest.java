package com.bonree.brfs.resourceschedule.config;

import static org.junit.Assert.*;

import org.junit.Test;
import org.quartz.Job;

import com.bonree.brfs.resourceschedule.gather.job.CalcResourceJob;

public class JobConfigTest {

	@Test
	public void testJobConfig() {
		JobConfig obj = new JobConfig();
		CalcResourceJob obj1 = new CalcResourceJob();
		try {
			obj.setJobClass("com.bonree.brfs.resourceschedule.config.JobConfig");
			fail("Test error Class but It's right");
		}
		catch (Exception e) {
			
		}
		try {
			obj.setJobClass("com.bonree.brfs.resourceschedule.gather.job.GatherResourceJob");
		}
		catch (Exception e) {
			fail("Test right Class but It's error   " +  e.getMessage());
			
		}
		
	}

}
