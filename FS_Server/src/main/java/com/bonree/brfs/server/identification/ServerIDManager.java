package com.bonree.brfs.server.identification;

import java.util.List;

import com.bonree.brfs.common.ZookeeperPaths;
import com.bonree.brfs.configuration.ServerConfig;
import com.bonree.brfs.server.identification.impl.ZookeeperServerIdOpt;

/*******************************************************************************
 * 版权信息：博睿宏远科技发展有限公司
 * Copyright: Copyright (c) 2007博睿宏远科技发展有限公司,Inc.All Rights Reserved.
 * 
 * @date 2018年3月27日 下午5:41:58
 * @Author: <a href=mailto:weizheng@bonree.com>魏征</a>
 * @Description: 管理Identification
 ******************************************************************************/
public class ServerIDManager {

    private ServerConfig serverConfig;

    private ServerIDOpt serverIDOpt;

    private FirstLevelServerID firstLevelServerID;

    private VirtualServerID virtualServerID;

    private final static String SINGLE_FILE = "/id/server_id";

    public ServerIDManager(ServerConfig config, ZookeeperPaths zkBasePaths) {
        this.serverConfig = config;
        this.serverIDOpt = ZookeeperServerIdOpt.getIdentificationServer(this.serverConfig.getZkHosts(), zkBasePaths.getBaseServerIdSeqPath());
       
        firstLevelServerID = new FirstLevelServerID(config.getZkHosts(), zkBasePaths.getBaseServerIdPath(), config.getHomePath() + SINGLE_FILE, serverIDOpt);
        firstLevelServerID.initOrLoadServerID();

        virtualServerID = new VirtualServerID(serverIDOpt);

    }

    public String getFirstServerID() {
        return firstLevelServerID.getServerID();
    }

    public String getSecondServerID(int storageIndex) {
        return firstLevelServerID.getSecondLevelServerID().getServerID(storageIndex);
    }

    public List<String> getVirtualServerID(int storageIndex, int count) {
        return virtualServerID.getServerId(storageIndex, count);
    }
}
