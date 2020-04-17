package com.bonree.brfs.client.route;

import static com.bonree.brfs.client.route.RouteAnalysis.analysisNormal;
import static com.bonree.brfs.client.route.RouteAnalysis.indexCode;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class RouteAnalysisTest {
    @Before
    public void init(){

    }
    /**
     * 方法 locateNormalServer 正常测试
     * NormalRouteV2内容：{"changeID":"123456","storageIndex":0,"secondID":"10","newSecondIDs":{"11":1,"12":2,"13":3},"version":"V2"}
     * uuid:"A"
     * 服务列表：["11"]
     * 预期 ：12
     */
    @Test
    public void testSingleNormalAnalysis(){
        Map<String,Integer> newSecondIds = new HashMap<>();
        newSecondIds.put("11",1);
        newSecondIds.put("12",2);
        newSecondIds.put("13",3);
        NormalRouterNode normalRoute = new NormalRouterNode("123456",0,"10",newSecondIds,"V2");

        List<String> services = Arrays.asList("11");
        String secondId = "10";
        String expectStr = "12";
        String uuid = "A";
        
        String dentStr = analysisNormal(indexCode(uuid), secondId, services, normalRoute);
        Assert.assertEquals(expectStr,dentStr);
    }
}
