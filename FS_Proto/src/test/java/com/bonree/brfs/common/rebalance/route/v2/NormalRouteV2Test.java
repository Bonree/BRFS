/*******************************************************************************
 * 版权信息： 北京博睿宏远数据科技股份有限公司
 * Copyright (c) 2007-2020 北京博睿宏远数据科技股份有限公司，Inc. All Rights Reserved.
 * @date 2020年03月13日 13:52:46
 * @author: <a href=mailto:zhucg@bonree.com>朱成岗</a>
 * @description: 解析路由规则测试类，采用Junit
 ******************************************************************************/

package com.bonree.brfs.common.rebalance.route.v2;

import com.bonree.brfs.common.rebalance.TaskVersion;
import com.bonree.brfs.common.rebalance.route.v1.NormalRoute;
import com.bonree.brfs.common.utils.JsonUtils;
import javafx.util.Pair;
import org.apache.commons.io.FileUtils;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class NormalRouteV2Test {
    private String resourcePath = this.getClass().getResource("/NormalRouteV2Set").getPath();
    private final String V1RouteFile = "V1_NormalRoute.json";
    private final String V2RouteFile = "V2_NormalRoute.json";
    @Before
    public void checkTestFile(){
        String filePath = null;
        filePath = resourcePath+ File.separator+V1RouteFile;
        if(!new File(filePath).exists()){
            throw new IllegalArgumentException(V1RouteFile+"not exists !!! test break!!!");
        }
        filePath = resourcePath+ File.separator+V2RouteFile;
        if(!new File(filePath).exists()){
            throw new IllegalArgumentException(V2RouteFile+"not exists !!! test break!!!");
        }
    }
    public byte[] readBytesFromFile(String fileName)throws IOException {
        String filePath = resourcePath+ File.separator+fileName;
        return FileUtils.readFileToByteArray(new File(filePath));
    }
    /**
     * 序列化V2版本的json
     * @throws IOException
     */
    @Test
    @Ignore
    public void testSerializeV2()throws Exception{
        String filePath =resourcePath+File.separator+"V2.json";
        System.out.println(filePath);
        String changeId = "123456";
        String  secondServer = "10";
        int storageId = 0;
        List<Pair<String,Integer>> serverid = new ArrayList<>();
        for(int index=11;index <14;index++){
            serverid.add(new Pair<String,Integer>(index+"",index%5));
        }
        NormalRouteV2 normalRouteV2 = new NormalRouteV2(changeId,storageId,secondServer,serverid, TaskVersion.V2);
        byte[] data = JsonUtils.toJsonBytesQuietly(normalRouteV2);
        FileUtils.writeByteArrayToFile(new File(filePath),data);
    }
    /**
     * 兼容反序列化V1版本的json
     * @throws IOException
     */
    @Test
    public void testCompatibleDeserializeV1()throws IOException{
        byte[] datas = readBytesFromFile(V1RouteFile);
        NormalRouteV2 routeV2 = JsonUtils.toObjectQuietly(datas,NormalRouteV2.class);
    }

    /**
     * 反序列化V2版本的json
     * @throws IOException
     */
    @Test
    public void testDeserializeV2()throws IOException{
        byte[] datas = readBytesFromFile(V2RouteFile);
        NormalRouteV2 routeV2 = JsonUtils.toObjectQuietly(datas,NormalRouteV2.class);
    }
}
