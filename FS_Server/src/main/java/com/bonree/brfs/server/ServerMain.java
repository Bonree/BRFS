package com.bonree.brfs.server;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bonree.brfs.configuration.Configuration;
import com.bonree.brfs.configuration.Configuration.ConfigException;

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.joran.JoranConfigurator;
import ch.qos.logback.core.joran.spi.JoranException;
import ch.qos.logback.core.util.StatusPrinter;

public class ServerMain {

    private static final Logger LOG = LoggerFactory.getLogger(ServerMain.class);

    static {
        // 加载 logback配置信息
        try {
            LoggerContext lc = (LoggerContext) LoggerFactory.getILoggerFactory();
            JoranConfigurator configurator = new JoranConfigurator();
            configurator.setContext(lc);
            lc.reset();
            configurator.doConfigure(Configuration.class.getResourceAsStream("/logback.xml"));
            StatusPrinter.printInCaseOfErrorsOrWarnings(lc);
        } catch (JoranException e) {
            e.printStackTrace();
            System.exit(0);
        }
    }

    public static void main(String[] args) {
        if (args.length < 1) {
            LOG.info("please specify config file!");
        }
        String configPath = args[0];
        LOG.info("load config file");
        Configuration configuration = Configuration.getInstance();
        try {
            configuration.parse(configPath);
        } catch (ConfigException e) {
            LOG.info("load configFile error!", e);
            System.exit(1);
        }
        
        LOG.info("begin launch server");
    }

    public static void help() {

    }

}
