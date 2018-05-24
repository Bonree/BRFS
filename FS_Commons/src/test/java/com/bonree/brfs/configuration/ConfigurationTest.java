package com.bonree.brfs.configuration;

import com.bonree.brfs.common.exception.ConfigParseException;
import com.bonree.brfs.configuration.Configuration;
import com.bonree.brfs.configuration.Configuration.ConfigPathException;

public class ConfigurationTest {

    public static void main(String[] args) throws ConfigPathException, ConfigParseException {
        Configuration configuration = Configuration.getInstance();

        configuration.printConfigDetail();

        configuration.parse("D:/gitwork/BRFS/config/server.properties");

        configuration.printConfigDetail();
        
        System.out.println(ServerConfig.parse(configuration,"."));
        System.out.println(StorageConfig.parse(configuration));

    }

}
