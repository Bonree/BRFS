package com.bonree.brfs.configuration;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bonree.brfs.server.utils.VerifyingFileFactory;

/*******************************************************************************
 * 版权信息：博睿宏远科技发展有限公司
 * Copyright: Copyright (c) 2007博睿宏远科技发展有限公司,Inc.All Rights Reserved.
 * 
 * @date 2018年3月13日 下午3:30:45
 * @Author: <a href=mailto:weizheng@bonree.com>魏征</a>
 * @Description: brfs 配置类，每个服务实例，只有一份配置信息，因此为单例
 ******************************************************************************/
public class Configuration {

    private static final Logger LOG = LoggerFactory.getLogger(Configuration.class);

    public static String CLUSTER_NAME = "cluster.name";

    public static String CLUSTER_NAME_VALUE = "default_brfs";

    public static String STORAGE_COMBINER_FILE_MAX_SIZE = "storage.combiner.file.max.size";

    public static String STORAGE_COMBINER_FILE_MAX_SIZE_VALUE = "64";

    public static String STORAGE_DATA_TTL = "storage.data.ttl";

    public static String STORAGE_DATA_TTL_VALUE = "30";

    public static String STORAGE_REPLICATION_NUMBER = "storage.replication.number";

    public static String STORAGE_REPLICATION_NUMBER_VALUE = "2";

    public static String STORAGE_REPLICATION_RECOVER = "storage.replication.recover";

    public static String STORAGE_REPLICATION_RECOVER_VALUE = "true";

    public static String STORAGE_DATA_ACHIVE = "storage.data.achive";

    public static String STORAGE_DATA_ACHIVE_VALUE = "false";

    public static String STORAGE_DATA_ACHIVE_DFS = "storage.data.achive.dfs";

    public static String STORAGE_DATA_ACHIVE_DFS_VALUE = "hdfs";

    public static String STORAGE_DATA_ACHIVE_AFTER_TIME = "storage.data.achive.after.time";

    public static String STORAGE_DATA_ACHIVE_AFTER_TIME_VALUE = "5";

    public static String GLOBAL_REPLICATION_RECOVER_AFTER_TIME = "global.replication.recover.after.time";

    public static String GLOBAL_REPLICATION_RECOVER_AFTER_TIME_VALUE = "3600";

    public static String NETWORK_HOST = "network.host";

    public static String NETWORK_HOST_VALUE = "127.0.0.1";

    public static String NETWORK_PORT = "network.port";

    public static String NETWORK_PORT_VALUE = "8880";

    public static String PATH_DATA = "path.data";

    public static String PATH_DATA_VALUE = "/home/brfs/data/";

    public static String PATH_LOGS = "path.logs";

    public static String PATH_LOGS_VALUE = "/home/brfs/log/";

    public static String ZOOKEEPER_NODES = "zookeeper.nodes";

    public static String ZOOKEEPER_NODES_VALUE = "localhost:2181";

    public static String ZOOKEEPER_SESSION_TIMEOUT = "zookeeper.session.timeout";

    public static String ZOOKEEPER_SESSION_TIMEOUT_VALUE = "5000";

    private String configFilePath = null;

    private volatile static Configuration configuration = null;

    private Map<String, String> configMap = new HashMap<String, String>();

    @SuppressWarnings("serial")
    public static class ConfigException extends Exception {
        public ConfigException(String msg) {
            super(msg);
        }

        public ConfigException(String msg, Exception e) {
            super(msg, e);
        }
    }

    public static Configuration getInstance() {
        if (configuration == null) {
            synchronized (Configuration.class) {
                if (configuration == null) {
                    configuration = new Configuration();
                }
            }
        }
        return configuration;
    }

    private Configuration() {
        configMap.put(CLUSTER_NAME, CLUSTER_NAME_VALUE);
        configMap.put(STORAGE_COMBINER_FILE_MAX_SIZE, STORAGE_COMBINER_FILE_MAX_SIZE_VALUE);
        configMap.put(STORAGE_DATA_TTL, STORAGE_DATA_TTL_VALUE);
        configMap.put(STORAGE_REPLICATION_NUMBER, STORAGE_REPLICATION_NUMBER_VALUE);
        configMap.put(STORAGE_REPLICATION_RECOVER, STORAGE_REPLICATION_RECOVER_VALUE);
        configMap.put(STORAGE_DATA_ACHIVE, STORAGE_DATA_ACHIVE_VALUE);
        configMap.put(STORAGE_DATA_ACHIVE_DFS, STORAGE_DATA_ACHIVE_DFS_VALUE);
        configMap.put(STORAGE_DATA_ACHIVE_AFTER_TIME, STORAGE_DATA_ACHIVE_AFTER_TIME_VALUE);
        configMap.put(GLOBAL_REPLICATION_RECOVER_AFTER_TIME, GLOBAL_REPLICATION_RECOVER_AFTER_TIME_VALUE);
        configMap.put(NETWORK_HOST, NETWORK_HOST_VALUE);
        configMap.put(NETWORK_PORT, NETWORK_PORT_VALUE);
        configMap.put(PATH_DATA, PATH_DATA_VALUE);
        configMap.put(PATH_LOGS, PATH_LOGS_VALUE);
        configMap.put(ZOOKEEPER_NODES, ZOOKEEPER_NODES_VALUE);
        configMap.put(ZOOKEEPER_SESSION_TIMEOUT, ZOOKEEPER_SESSION_TIMEOUT_VALUE);
    }

    public String getConfigFilePath() {
        return configFilePath;
    }

    public void parse(String path) throws ConfigException {
        LOG.info("Reading configuration from: " + path);

        try {
            File configFile = (new VerifyingFileFactory.Builder(LOG).warnForRelativePath().failForNonExistingPath().build()).create(path);
            Properties cfg = new Properties();
            FileInputStream in = new FileInputStream(configFile);
            try {
                cfg.load(in);
                configFilePath = path;
            } finally {
                in.close();
            }

            parseProperties(cfg);

        } catch (IOException e) {
            throw new ConfigException("Error processing " + path, e);
        } catch (IllegalArgumentException e) {
            throw new ConfigException("Error processing " + path, e);
        }
    }

    private void parseProperties(Properties cfg) {

        for (Entry<Object, Object> entry : cfg.entrySet()) {
            String key = entry.getKey().toString().trim();
            String value = entry.getValue().toString().trim();
            configMap.put(key, value);
        }
    }

    public Configuration setProperty(String key, String value) {
        configMap.put(key, value);
        return this;
    }

    public Configuration setProperty(Object key, Object value) {
        configMap.put(key.toString().trim(), value.toString().trim());
        return this;
    }

    public String getProperty(String key) {
        return configMap.get(key);
    }

    public Object getProperty(Object key) {
        return configMap.get(key);
    }

    public void printConfigDetail() {
        StringBuffer configStr = new StringBuffer();
        configStr.append(System.getProperty("line.separator"));

        for (Entry<String, String> entry : configMap.entrySet()) {
            String key = entry.getKey();
            String value = entry.getValue();
            configStr.append(key + "=" + value);
            configStr.append(System.getProperty("line.separator"));
        }
        LOG.info(configStr.toString());
    }
}
