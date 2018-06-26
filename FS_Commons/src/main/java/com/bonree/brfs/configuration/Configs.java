package com.bonree.brfs.configuration;

public class Configs {
	private static final ConfigObj CONFIG;

	static {
		String configFileName = System.getProperty(SystemProperties.PROP_CONFIGURATION_FILE);
		if (configFileName == null) {
			throw new RuntimeException("no configuration file is specified, provide by using property[" + SystemProperties.PROP_CONFIGURATION_FILE + "]");
		}

		CONFIG = ConfigurationLoader.load(configFileName);
	}
	
	public static ConfigObj getConfiguration() {
		return CONFIG;
	}
}
