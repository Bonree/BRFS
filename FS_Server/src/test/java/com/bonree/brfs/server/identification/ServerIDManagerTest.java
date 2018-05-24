package com.bonree.brfs.server.identification;

import java.util.List;

import com.bonree.brfs.common.ZookeeperPaths;
import com.bonree.brfs.common.exception.ConfigParseException;
import com.bonree.brfs.configuration.Configuration;
import com.bonree.brfs.configuration.Configuration.ConfigPathException;
import com.bonree.brfs.configuration.ServerConfig;

public class ServerIDManagerTest {
    public static final String CONFIG_NAME = "D:/gitwork/BRFS/config/server.properties";
    public static final String HOME = "D:/gitwork/BRFS";

    public static void main(String[] args) {
        
        try {
            Configuration conf = Configuration.getInstance();
            conf.parse(CONFIG_NAME);
            conf.printConfigDetail();
            ServerConfig serverConfig=ServerConfig.parse(conf, HOME);
            ZookeeperPaths zookeeperPaths = ZookeeperPaths.create(serverConfig.getClusterName(), serverConfig.getZkHosts());
            ServerIDManager idManager = new ServerIDManager(serverConfig, zookeeperPaths);
            String firstServerID = idManager.getFirstServerID();
            System.out.println(firstServerID);
            String secondServerID=idManager.getSecondServerID(2);
            System.out.println(secondServerID);
            List<String> virtualServerIDs = idManager.listNormalVirtualID(1);
            System.out.println(virtualServerIDs);
//            idManager.deleteVirtualID(1, "30");
            virtualServerIDs = idManager.listNormalVirtualID(1);
            virtualServerIDs=idManager.getVirtualServerID(1, 2);
            System.out.println(virtualServerIDs);
            
        } catch (ConfigPathException e) {
            e.printStackTrace();
            System.exit(1);
        } catch (ConfigParseException e) {
            e.printStackTrace();
        }
    }
}
