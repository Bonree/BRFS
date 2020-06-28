package com.bonree.brfs.client.route;

import static com.bonree.brfs.client.route.RouteAnalysis.analysisNormal;
import static com.bonree.brfs.client.route.RouteAnalysis.indexCode;

import com.bonree.brfs.common.data.utils.JsonUtils;
import com.google.common.base.Strings;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class RouteAnalysisTest {
    @Before
    public void init() {

    }

    /**
     * 方法 locateNormalServer 正常测试
     * NormalRouteV2内容：{"changeID":"123456","storageIndex":0,"secondID":"10","newSecondIDs":{"11":1,"12":2,"13":3},"version":"V2"}
     * uuid:"A"
     * 服务列表：["11"]
     * 预期 ：12
     */
    @Test
    public void testSingleNormalAnalysis() {
        Map<String, Integer> newSecondIds = new HashMap<>();
        newSecondIds.put("11", 1);
        newSecondIds.put("12", 2);
        newSecondIds.put("13", 3);
        Map<String, String> secondShip = new HashMap<>();
        secondShip.put("11", "11");
        secondShip.put("12", "11");
        secondShip.put("13", "13");
        NormalRouterNode normalRoute = new NormalRouterNode("123456", 0, "10", newSecondIds, secondShip, "V2");

        List<String> services = Arrays.asList("11");
        String secondId = "10";
        String expectStr = "13";
        String uuid = "A";

        String dentStr = analysisNormal(indexCode(uuid), secondId, services, normalRoute);
        Assert.assertEquals(expectStr, dentStr);
    }

    @Test
    public void analysisTest() throws Exception {
        String content =
            "[{\"type\":\"normal\",\"changeId\":\"1592998124fe0fc682-7735-4380-acd0-c1e7a6326d04\","
                + "\"storageRegionIndex\":0,\"baseSecondId\":\"20\","
                + "\"newSecondIDs\":{\"22\":140604080,\"23\":200149420,\"21\":200149440},"
                + "\"secondFirstShip\":{\"22\":\"13\",\"23\":\"14\",\"20\":\"12\",\"21\":\"13\"},"
                + "\"version\":\"V2\"},\n"
                + "{\"type\":\"normal\",\"changeId\":\"1592998376a3d55a09-853f-44a4-b3db-ee7563abd539\","
                + "\"storageRegionIndex\":0,\"baseSecondId\":\"21\","
                + "\"newSecondIDs\":{\"23\":200149152,\"24\":200149128},"
                + "\"secondFirstShip\":{\"22\":\"13\",\"23\":\"14\",\"24\":\"12\",\"21\":\"13\"},"
                + "\"version\":\"V2\"}]";
        NormalRouterNode[] array = JsonUtils.toObject(content, NormalRouterNode[].class);
        Map<String, NormalRouterNode> map = new HashMap<>();
        for (NormalRouterNode x : Arrays.asList(array)) {
            map.put(x.getBaseSecondId(), x);
            System.out.println(JsonUtils.toJsonString(x));
        }
        List<String> services = Arrays.asList("20", "23");
        String uuid = "285e64dd994e4cc7acc7f053e9b0591a";
        int code = RouteAnalysis.indexCode(uuid);
        String second = finalServerId(code, "20", services, map, new HashMap<>());
        System.out.println(second);
    }

    private String finalServerId(
        int code,
        String serverId,
        List<String> secondServerIdList,
        Map<String, NormalRouterNode> normalMapper,
        Map<String, VirtualRouterNode> virtualMapper) {
        String secondId = serverId;
        if (serverId.startsWith("3")) {
            //virtual id
            VirtualRouterNode update = virtualMapper.get(serverId);
            if (update == null) {
                return serverId;
            }

            secondId = update.getNewSecondId();
        }

        if (Strings.isNullOrEmpty(secondId)) {
            return serverId;
        }

        NormalRouterNode normalUpdate;
        while ((normalUpdate = normalMapper.get(secondId)) != null) {
            secondId = RouteAnalysis.analysisNormal(code, secondId, secondServerIdList, normalUpdate);
        }

        return secondId;
    }

}
