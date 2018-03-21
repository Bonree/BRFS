
package com.bonree.brfs.resourceschedule;

import java.sql.Date;
import java.util.ArrayList;

import org.quartz.Job;
import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.JobKey;

public class test implements Job {
	String jobSays;
	float myFloatValue;
	ArrayList state;

	public test() {
	}

	@Override
	public void execute(JobExecutionContext context) throws JobExecutionException {
		JobKey key = context.getJobDetail().getKey();
		JobDataMap dataMap = context.getMergedJobDataMap();
		// Note the difference from the previous example 
		state.add(new Date(0));
		System.err.println("Instance " + key + " of DumbJob says: " + jobSays + ", and val is: " + myFloatValue);
	}

	public void setJobSays(String jobSays) {
		this.jobSays = jobSays;
	}

	public void setMyFloatValue(float myFloatValue) {
		myFloatValue = myFloatValue;
	}

	public void setState(ArrayList state) {
		state = state;
	}
}
