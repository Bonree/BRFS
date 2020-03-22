package com.bonree.brfs.identification;

import com.bonree.brfs.rebalance.route.RouteParserTest;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryNTimes;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * 版权信息: 北京博睿宏远数据科技股份有限公司
 * Copyright: Copyright (c) 2007-2020 北京博睿宏远数据科技股份有限公司,Inc.All Rights Reserved.
 *
 * @date: 2020年03月22日 12:53
 * @author: <a href=mailto:zhucg@bonree.com>朱成岗</a>
 * @description: 测试磁盘id生成的逻辑是否有效
 **/
public class DiskNodeIDImplTest {
    private static String ID_BAS_PATH="/brfsDevTest/identification/DiskNodes";
    private static String ZKADDRES = RouteParserTest.ZK_ADDRESS;
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

    /**
     * TestCase 1:  测试构建实例是否报错
     */
    @Test
    public void newConstructorTest(){
        DiskNodeIDImpl impl = new DiskNodeIDImpl(framework,ID_BAS_PATH);
    }

    /**
     * TestCase 2: 测试获取磁盘节点唯一id
     */
    @Test
    public void getLevelTest(){
        DiskNodeIDImpl impl = new DiskNodeIDImpl(framework,ID_BAS_PATH);
        System.out.println(impl.genLevelID());

    }

    @After
    public void closeAll(){
        if(framework != null){
            framework.close();
        }
    }

}
