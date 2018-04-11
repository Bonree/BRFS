package com.bonree.brfs.server.identification;

import java.util.List;

import com.bonree.brfs.server.utils.FileUtils;
import com.google.common.collect.Lists;

public class FirstLevelServerID {

    private ServerIDGen serverIdGen;

    String sigleFile;

    public FirstLevelServerID(String sigleFile, ServerIDGen serverIdGen) {
        this.sigleFile = sigleFile;
        this.serverIdGen = serverIdGen;
    }

    /** 概述：单副本serverID不需要判断是否过期
     * @return
     * @user <a href=mailto:weizheng@bonree.com>魏征</a>
     */
    public String getServerId() {
        String serverId = null;
        // 文件不存在，则说明为新的服务，需要生成serverID，并保存
        if (!FileUtils.isExist(sigleFile)) {
            FileUtils.createFile(sigleFile, true);
            serverId = serverIdGen.genSingleIdentification();
            List<String> contents = Lists.newArrayList(serverId);
            FileUtils.writeFileFromList(sigleFile, contents);
        } else {
            List<String> contents = FileUtils.readFileByLine(sigleFile);
            if (contents.isEmpty()) {
                serverId = serverIdGen.genSingleIdentification();
                contents = Lists.newArrayList(serverId);
                FileUtils.writeFileFromList(sigleFile, contents);
            } else {
                serverId = contents.get(0);
            }
        }
        return serverId;
    }

}
