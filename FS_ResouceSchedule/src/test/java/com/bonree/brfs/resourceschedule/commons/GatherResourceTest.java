package com.bonree.brfs.resourceschedule.commons;

import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bonree.brfs.common.utils.JsonUtils;
import com.bonree.brfs.configuration.Configuration;
import com.bonree.brfs.resourceschedule.model.BaseMetaServerModel;
import com.bonree.brfs.resourceschedule.model.ResourceModel;
import com.bonree.brfs.resourceschedule.model.StatServerModel;
import com.bonree.brfs.resourceschedule.model.StateMetaServerModel;
import com.bonree.brfs.resourceschedule.utils.LibUtilsTest;

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.joran.JoranConfigurator;
import ch.qos.logback.core.joran.spi.JoranException;
import ch.qos.logback.core.util.StatusPrinter;

public class GatherResourceTest {
	@Test
	public void testGatherResource() {
		try {
			LibUtilsTest.initLibrary();
			BaseMetaServerModel base = GatherResource.gatherBase("Well", "E:/", Arrays.asList(new String[]{
					"192.168.4.177"
			}));
			String baseJson = JsonUtils.toJsonString(base);
			System.out.println(baseJson);
			StateMetaServerModel tmp = GatherResource.gatherResource("E:/", Arrays.asList(new String[]{
					"192.168.4.177"
			}));
	
			String resourceJson = JsonUtils.toJsonString(tmp);
//			System.out.println(resourceJson);
	
			Queue<StateMetaServerModel> a = new ConcurrentLinkedQueue<StateMetaServerModel>();
			for(int i =0; i< 3;i++){
				StateMetaServerModel stat = GatherResource.gatherResource("E:/", Arrays.asList(new String[]{
						"192.168.4.177"
				}));
				a.add(stat);
				Thread.sleep(1000);
			}
			List<StatServerModel> sList = GatherResource.calcState(a);
			if(sList == null || sList.isEmpty()){
				fail("list is empty");
			}
			StatServerModel sCurrent = GatherResource.calcStatServerModel(sList, Arrays.asList(new String[]{
					"E:\\zhuchenggang",
			}), 1000l);
			String ssJson = JsonUtils.toJsonString(sCurrent);
			System.out.println(ssJson);
//			System.out.println("sCurrent remain size "+ sCurrent.getRemainDiskSize() + " - " + sCurrent.getRemainDiskSize());
			ResourceModel resource = GatherResource.calcResourceValue(base, sCurrent);
//			System.out.println("resource remain size"+ resource.getDiskRemainRate() + " taotal " + resource.getDiskSize());
			String rsJson = JsonUtils.toJsonString(resource);
			System.out.println(rsJson);
			
		}
		catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	

}
