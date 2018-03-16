package com.bonree.brfs.resourceschedule.service;

import static org.junit.Assert.*;

import java.io.File;
import java.io.InputStream;

import org.junit.Test;
import org.quartz.SchedulerException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bonree.brfs.resourceschedule.service.impl.QuartzServer;

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.joran.JoranConfigurator;
import ch.qos.logback.core.joran.spi.JoranException;
import ch.qos.logback.core.util.StatusPrinter;

public class QuartzServerTest {
	private static Logger logger = LoggerFactory.getLogger("Computer");
    static {
        // 加载 logback配置信息
        try {
            LoggerContext lc = (LoggerContext) LoggerFactory.getILoggerFactory();
            JoranConfigurator configurator = new JoranConfigurator();
            configurator.setContext(lc);
            lc.reset();
            String path = ClassLoader.getSystemResource(".").getPath() + "logback.xml";
            File file = new File(path);
            if(file.exists()){
            	configurator.doConfigure(file);
//            	configurator.doConfigure(QuartzServerTest.class.getResourceAsStream("/logback.xml"));
            }
            System.out.println(file.getAbsolutePath());
            StatusPrinter.printInCaseOfErrorsOrWarnings(lc);
        } catch (JoranException e) {
            e.printStackTrace();
            System.exit(0);
        }
    }
	@Test
	public void testStartServer() {
		try {
			assertEquals(true,QuartzServer.instance.startServer());
			Thread.sleep(5000);
		}
		catch (SchedulerException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
