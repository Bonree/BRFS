/*******************************************************************************
 * 版权信息： 北京博睿宏远数据科技股份有限公司
 * Copyright (c) 2007-2020 北京博睿宏远数据科技股份有限公司，Inc. All Rights Reserved.
 * @date 2020年03月19日 11:44:38
 * @author: <a href=mailto:zhucg@bonree.com>朱成岗</a>
 * @description: 路由规则工厂生成类
 ******************************************************************************/

package com.bonree.brfs.rebalance.route.factory;

import com.bonree.brfs.common.rebalance.TaskVersion;
import com.bonree.brfs.common.rebalance.route.NormalRouteInterface;
import java.io.File;
import java.io.IOException;
import org.apache.commons.io.FileUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class SingleRouteFactoryTest {
    private String resourcePath = this.getClass().getResource("/Routes").getPath();
    private String v1File = "V1_NormalRoute.json";
    private String v2File = "V2_NormalRoute.json";
    private String v3File = "V3_NormalRoute.json";
    private String noVersionFile = "No_Version_NormalRoute.json";
    private String noFile = "NO_NormalRoute.json";
    private String otherFile = "OtherJson.json";
    private String virtualFile = "VirtualRoute.json";

    /**
     * 检查测试资源，若不存在则测试不进行
     */
    @Before
    public void checkTestFile() {
        checkFile(v1File);
        checkFile(v2File);
        checkFile(v3File);
        checkFile(noVersionFile);
        checkFile(noFile);
        checkFile(otherFile);
    }

    public void checkFile(String name) {
        String filePath = null;
        filePath = resourcePath + File.separator + name;
        if (!new File(filePath).exists()) {
            throw new IllegalArgumentException(name + "not exists !!! test break!!!");
        }
    }

    public byte[] readBytesFromFile(String fileName) {
        String filePath = resourcePath + File.separator + fileName;
        try {
            return FileUtils.readFileToByteArray(new File(filePath));
        } catch (IOException e) {
            Assert.fail("read file happen error !! file:" + filePath);
        }
        return null;
    }

    /**
     * TestCase 1: 反序列V2版本的路由规则
     */
    @Test
    public void deserializeV2() {
        byte[] data = readBytesFromFile(v2File);
        NormalRouteInterface route = SingleRouteFactory.createRoute(data);
        Assert.assertEquals(TaskVersion.V2, route.getRouteVersion());
    }

    /**
     * TestCase 2: 反序列化V1版本的路由规则
     */
    @Test
    public void deserializeV1() {
        byte[] data = readBytesFromFile(v1File);
        NormalRouteInterface route = SingleRouteFactory.createRoute(data);
        System.out.println(route.getRoutes());
        Assert.assertEquals(TaskVersion.V1, route.getRouteVersion());
    }

    /**
     * TestCase 3: 反序列化高于V2版本的路由规则
     */
    @Test(expected = IllegalArgumentException.class)
    public void deserializeBiggerV2() {
        byte[] data = readBytesFromFile(v3File);
        SingleRouteFactory.createRoute(data);
    }

    /**
     * TestCase 4: 反序列话化无版本信息的路由规则
     */
    @Test(expected = IllegalArgumentException.class)
    public void deserializeNoVersion() {
        byte[] data = readBytesFromFile(noVersionFile);
        SingleRouteFactory.createRoute(data);
    }

    /**
     * TestCase 5: 反序列化非json数据
     */
    @Test(expected = IllegalArgumentException.class)
    public void deserializeNoJson() {
        byte[] data = readBytesFromFile(noFile);
        SingleRouteFactory.createRoute(data);
    }

    /**
     * TestCase 6: 反序列化非路由规则json数据
     */
    @Test(expected = IllegalArgumentException.class)
    public void deserializeOtherJson() {
        byte[] data = readBytesFromFile(otherFile);
        SingleRouteFactory.createRoute(data);
    }

    /**
     * TestCase 7: 虚拟id迁移规则的反序列化
     *
     * @throws Exception
     */
    @Test
    public void deserializeVirtualRoute() throws Exception {
        byte[] data = readBytesFromFile(virtualFile);
        SingleRouteFactory.createVirtualRoute(data);
    }
}
