package com.bonree.brfs.server.identification;

import java.util.List;

import org.codehaus.jackson.map.DeserializerFactory.Config;

import com.bonree.brfs.common.ZookeeperPaths;
import com.bonree.brfs.configuration.ServerConfig;
import com.bonree.brfs.server.identification.impl.ZookeeperServerIdGen;
import com.bonree.brfs.server.utils.FileUtils;
import com.google.common.collect.Lists;

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

    private ServerIDGen identification;

    private final static String SINGLE_FILE = "/id/single_id";
    
    private final static String MULTI_FILE = "/id/multi_id";

    public ServerIDManager(ServerConfig config, ZookeeperPaths basePaths) {
        this.serverConfig = config;
        this.identification = ZookeeperServerIdGen.getIdentificationServer(this.serverConfig.getZkNodes(), basePaths.getBaseServerIdPath());
    }


    public String getMultiServerId() {
        String serverId = null;
        String multiFile = serverConfig.getHomePath() + SINGLE_FILE;
        if (!FileUtils.isExist(multiFile)) {

        } else {

        }
        return null;
    }

}
