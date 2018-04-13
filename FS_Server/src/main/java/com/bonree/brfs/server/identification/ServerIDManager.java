package com.bonree.brfs.server.identification;

import java.util.List;

import com.bonree.brfs.common.ZookeeperPaths;
import com.bonree.brfs.configuration.ServerConfig;
import com.bonree.brfs.server.identification.impl.FirstLevelServerIDImpl;
import com.bonree.brfs.server.identification.impl.VirtualServerIDImpl;

/*******************************************************************************
 * 版权信息：博睿宏远科技发展有限公司
 * Copyright: Copyright (c) 2007博睿宏远科技发展有限公司,Inc.All Rights Reserved.
 * 
 * @date 2018年3月27日 下午5:41:58
 * @Author: <a href=mailto:weizheng@bonree.com>魏征</a>
 * @Description: 管理Identification
 ******************************************************************************/
public class ServerIDManager {

    private FirstLevelServerIDImpl firstLevelServerID;

    private VirtualServerID virtualServerID;

    private final static String SINGLE_FILE = "/id/server_id";

    public ServerIDManager(ServerConfig config, ZookeeperPaths zkBasePaths) {
        firstLevelServerID = new FirstLevelServerIDImpl(config.getZkHosts(), zkBasePaths.getBaseServerIdPath(), config.getHomePath() + SINGLE_FILE, zkBasePaths.getBaseServerIdSeqPath());
        firstLevelServerID.initOrLoadServerID();
        virtualServerID = new VirtualServerIDImpl(config.getZkHosts(), zkBasePaths.getBaseServerIdSeqPath());

    }

    public String getFirstServerID() {
        return firstLevelServerID.getServerID();
    }

    public String getSecondServerID(int storageIndex) {
        return firstLevelServerID.getSecondLevelServerID().getServerID(storageIndex);
    }

    public List<String> getVirtualServerID(int storageIndex, int count) {
        return virtualServerID.getVirtualID(storageIndex, count);
    }

    public boolean invalidVirtualID(int storageIndex, String id) {
        return virtualServerID.invalidVirtualIden(storageIndex, id);
    }

    public boolean deleteVirtualID(int storageIndex, String id) {
        return virtualServerID.deleteVirtualIden(storageIndex, id);
    }

    public List<String> listVirtualID(int storageIndex) {
        return virtualServerID.listNormalVirtualID(storageIndex);
    }
}
