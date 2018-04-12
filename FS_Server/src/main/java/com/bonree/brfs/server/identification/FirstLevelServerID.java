package com.bonree.brfs.server.identification;

import java.util.List;

import com.bonree.brfs.common.zookeeper.curator.CuratorClient;
import com.bonree.brfs.server.utils.FileUtils;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

public class FirstLevelServerID {

    private ServerIDOpt serverIDOpt;

    private String firstServerID;

    private String firstServerIDFile;

    private String zkHosts;

    private String firstZKPath;

    private SecondLevelServerID secondServerID;

    private boolean newServer = true;

    public FirstLevelServerID(String zkHosts, String firstZKPath, String firstServerIDFile, ServerIDOpt serverIDOpt) {
        this.zkHosts = zkHosts;
        this.firstZKPath = firstZKPath;
        this.firstServerIDFile = firstServerIDFile;
        this.serverIDOpt = serverIDOpt;
    }

    /** 概述：加载一级ServerID
     * 一级ServerID是用于标识每个服务的，不同的服务的一级ServerID一定是不同的，
     * 所以不会出现线程安全的问题
     * @return
     * @user <a href=mailto:weizheng@bonree.com>魏征</a>
     */
    public void initOrLoadServerID() {
        // 文件不存在，则说明为新的服务，需要生成serverID，并保存
        CuratorClient client = null;
        try {
            client = CuratorClient.getClientInstance(zkHosts);
            if (!FileUtils.isExist(firstServerIDFile)) {
                FileUtils.createFile(firstServerIDFile, true);
                firstServerID = serverIDOpt.genFirstIdentification();
                List<String> contents = Lists.newArrayList(firstServerID);
                FileUtils.writeFileFromList(firstServerIDFile, contents);
            } else {
                List<String> contents = FileUtils.readFileByLine(firstServerIDFile);
                if (contents.isEmpty()) {
                    firstServerID = serverIDOpt.genFirstIdentification();
                    contents = Lists.newArrayList(firstServerID);
                    FileUtils.writeFileFromList(firstServerIDFile, contents);
                } else {
                    newServer = false;
                    firstServerID = contents.get(0);
                }

            }
            // 检查zk上是否一致
            String firstIDNode = firstZKPath + '/' + firstServerID;
            if (!client.checkExists(firstIDNode)) {
                client.createPersistent(firstIDNode, false);
            }
        } finally {
            if (client != null) {
                client.close();
            }
        }
        secondServerID = new SecondLevelServerID(zkHosts, firstZKPath + '/' + firstServerID, serverIDOpt);
        secondServerID.loadServerID();
    }

    /** 概述：从缓存中返回本服务的一级ServerID
     * @return
     * @user <a href=mailto:weizheng@bonree.com>魏征</a>
     */
    public String getServerID() {
        if (firstServerID != null) {
            return firstServerID;
        } else {
            throw new IllegalStateException("first Server ID not init or occur a excrption!!");
        }
    }

    /** 概述：在加载一级ServerID的时候，可以判断是否为newServer
     * @return
     * @user <a href=mailto:weizheng@bonree.com>魏征</a>
     */
    public boolean isNewServer() {
        return newServer;
    }

    public SecondLevelServerID getSecondLevelServerID() {
        return Preconditions.checkNotNull(secondServerID, "second Server ID load fail!!!");
    }

}
