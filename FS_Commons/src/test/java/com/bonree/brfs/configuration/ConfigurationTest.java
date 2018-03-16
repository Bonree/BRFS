package com.bonree.brfs.configuration;

import com.bonree.brfs.configuration.Configuration;
import com.bonree.brfs.configuration.Configuration.ConfigException;

public class ConfigurationTest {

    public static void main(String[] args) throws ConfigException {
        Configuration configuration = Configuration.getInstance();

        configuration.printConfigDetail();

        configuration.parse("D:/gitwork/BRFS/config/server.properties");

        configuration.printConfigDetail();

    }

}
