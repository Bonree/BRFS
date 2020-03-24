/*******************************************************************************
 * 版权信息： 北京博睿宏远数据科技股份有限公司
 * Copyright (c) 2007-2020 北京博睿宏远数据科技股份有限公司，Inc. All Rights Reserved.
 * @date 2020年03月23日 11:35:07
 * @author: <a href=mailto:zhucg@bonree.com>朱成岗</a>
 * @description: 磁盘id生成逻辑测试
 ******************************************************************************/

package com.bonree.brfs.identification;

import com.bonree.brfs.rebalance.route.RouteParserTest;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryNTimes;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;

public class DiskDaemonTest {
    private static String FILE_DIR = DiskDaemonTest.class.getResource("/DiskNodeId").getPath();
    private static String ID_BAS_PATH="/brfsDevTest/identification/server_ids";
    private static String ID_SEQ_PATH="/brfsDevTest/identification/server_id_sequences";
    private static String ZKADDRES = RouteParserTest.ZK_ADDRESS;
    private static String DISK_NODE_NAME="DiskNodeIDs";
    private CuratorFramework framework = null;
    @Before
    public void checkZK(){
        framework = CuratorFrameworkFactory.newClient(ZKADDRES,new RetryNTimes(5,300));
        framework.start();
        try {
            framework.blockUntilConnected();
        } catch (InterruptedException e) {
            Assert.fail("zookeeper client is invaild !! address: "+ZKADDRES);
        }
    }
    @Test
    public void initTest(){
        DiskDaemon impl = new DiskDaemon(framework,ID_BAS_PATH,FILE_DIR+ File.separator+DISK_NODE_NAME,ID_SEQ_PATH);
    }
    @After
    public void closeAll(){
        if(framework != null){
            framework.close();
        }
    }
}
