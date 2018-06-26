package com.bonree.brfs.configuration;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.configuration2.FileBasedConfiguration;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.configuration2.builder.FileBasedConfigurationBuilder;
import org.apache.commons.configuration2.builder.fluent.Parameters;
import org.apache.commons.configuration2.ex.ConfigurationException;

import com.google.common.base.Charsets;

/**
 * 从配置文件加载配置内容
 * 
 * @author yupeng
 *
 */
public final class ConfigurationLoader {
	private static final FileBasedConfigurationBuilder<FileBasedConfiguration> CONFIGURATION_BUILDER;
	
	private static final Map<String, ConfigObj> CONFIG_CACHE;
	
	static {
		CONFIGURATION_BUILDER = new FileBasedConfigurationBuilder<FileBasedConfiguration>(PropertiesConfiguration.class);
		CONFIG_CACHE = new HashMap<String, ConfigObj>();
	}
	
	/**
	 * 加载配置文件，如果之前已经读取过，则返回之前读取的内容
	 * 
	 * @param fileName 配置文件路径
	 * @return
	 */
	public static ConfigObj load(String fileName) {
		return load(fileName, true);
	}
	
	/**
	 * 加载配置文件，如果指定不使用缓存，每次都会重新加载
	 * 
	 * @param fileName 配置文件路径
	 * @param useCache 是否使用缓存功能
	 * @return
	 */
	public static ConfigObj load(String fileName, boolean useCache) {
		if(fileName == null) {
			throw new NullPointerException("config file name is null.");
		}
		
		ConfigObj config = null;
		
		if(useCache) {
			config = CONFIG_CACHE.get(fileName);
		}
		
		if(config != null) {
			return config;
		}
		
		try {
			File configFile = new File(fileName);
			if(!configFile.exists() || !configFile.isFile()) {
				throw new RuntimeException("configuration file[" + fileName + "] is not valid.");
			}
			
			Parameters params = new Parameters();
			CONFIGURATION_BUILDER.resetResult();
			CONFIGURATION_BUILDER.configure(params.properties()
	        				.setFileName(fileName)
	        				.setEncoding(Charsets.UTF_8.name())
	        				.setThrowExceptionOnMissing(true));
			
			config = new ConfigObj(CONFIGURATION_BUILDER.getConfiguration());
	        
			return config;
		} catch (ConfigurationException e) {
			throw new RuntimeException("load configuration file error", e);
		} finally {
			if(config != null) {
				CONFIG_CACHE.put(fileName, config);
			}
		}
		
	}
}
