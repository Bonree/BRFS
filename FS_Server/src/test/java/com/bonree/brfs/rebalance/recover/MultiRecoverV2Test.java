package com.bonree.brfs.rebalance.recover;

import com.bonree.brfs.common.rebalance.route.NormalRouteInterface;
import com.bonree.brfs.common.rebalance.route.VirtualRoute;
import com.bonree.brfs.common.rebalance.route.impl.v2.NormalRouteV2;
import com.bonree.brfs.common.utils.JsonUtils;
import com.bonree.brfs.common.utils.Pair;
import com.bonree.brfs.rebalance.route.BlockAnalyzer;
import com.bonree.brfs.rebalance.route.RouteLoader;
import com.bonree.brfs.rebalance.route.impl.RouteParser;
import com.bonree.brfs.rebalance.task.BalanceTaskSummary;
import java.util.Collection;
import java.util.List;
import org.junit.Test;

public class MultiRecoverV2Test {
    @Test
    public void testRouteToChange() throws Exception {
        String content = "{\n"
            +
            "  \"newSecondIds\": {\n"
            +
            "    \"21\": 199266684\n"
            +
            "  },\n"
            +
            "  \"secondFirstShip\": {\n"
            +
            "    \"20\": \"11\",\n"
            +
            "    \"21\": \"10\"\n"
            +
            "  },\n"
            +
            "  \"id\": \"7c9e2e34-0b90-4baf-af4d-5dd1beb1edba\",\n"
            +
            "  \"changeID\": \"1589786418cf2e0cbd-5ac1-4237-b836-da77d88456ac\",\n"
            +
            "  \"serverId\": \"20\",\n"
            +
            "  \"storageIndex\": 0,\n"
            +
            "  \"partitionId\": \"41\",\n"
            +
            "  \"taskType\": \"NORMAL\",\n"
            +
            "  \"taskStatus\": \"RUNNING\",\n"
            +
            "  \"outputServers\": [\n"
            +
            "    \"21\"\n"
            +
            "  ],\n"
            +
            "  \"inputServers\": [\n"
            +
            "    \"21\",\n"
            +
            "    \"22\"\n"
            +
            "  ],\n"
            +
            "  \"aliveServer\": [\n"
            +
            "    \"21\",\n"
            +
            "    \"22\"\n"
            +
            "  ],\n"
            +
            "  \"delayTime\": 300,\n"
            +
            "  \"interval\": -1\n"
            +
            "}";
        BalanceTaskSummary balanceSummary = JsonUtils.toObject(content.getBytes(), BalanceTaskSummary.class);
        NormalRouteV2 routeV2 =
            new NormalRouteV2(balanceSummary.getChangeID(), balanceSummary.getStorageIndex(), balanceSummary.getServerId(),
                              balanceSummary.getNewSecondIds(), balanceSummary.getSecondFirstShip());

        RouteLoader loader = new RouteLoader() {
            @Override
            public Collection<VirtualRoute> loadVirtualRoutes(int storageRegionId) throws Exception {
                return null;
            }

            @Override
            public Collection<NormalRouteInterface> loadNormalRoutes(int storageRegionId) throws Exception {
                return null;
            }
        };
        RouteParser parser = new RouteParser(0, loader);
        String fileName = "97872496313e40289611d44ff6c5b041_21_20";
        Pair<String, List<String>> fileInfoPair = BlockAnalyzer.analyzingFileName(fileName);
        int fileCode = BlockAnalyzer.sumName(fileInfoPair.getFirst());
        List<String> excludes = fileInfoPair.getSecond();
        System.out.println(routeV2);
        String selector = routeV2.locateNormalServer(fileCode, excludes);
        System.out.println(selector);

    }
}
