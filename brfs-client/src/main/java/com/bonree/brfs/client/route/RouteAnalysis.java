package com.bonree.brfs.client.route;

import static java.util.stream.Collectors.toList;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

public final class RouteAnalysis {

    public static int indexCode(String s) {
        int sum = 0;
        for (int i = 0; i < s.length(); i++) {
            sum = sum + s.charAt(i);
        }

        return sum;
    }

    public static String analysisNormal(
        int code,
        String secondId,
        List<String> services,
        NormalRouterNode routerNode) {
        Map<String, Integer> map = routerNode.getNewSecondIDs();
        if (map == null || map.isEmpty()) {
            return secondId;
        }

        List<String> selector = filterService(map.keySet(), services, routerNode);
        if (selector == null || selector.isEmpty()) {
            return secondId;
        }

        int weightValue = calcWeight(selector, map);
        if (weightValue <= 0) {
            return secondId;
        }

        int weightIndex = hashFileName(code, weightValue);
        int index = searchIndex(selector, map, weightIndex);
        return selector.get(index);
    }

    /**
     * 过滤不参与的服务
     *
     * @param newSecondIDs
     * @param services
     *
     * @return
     */
    private static List<String> filterService(
        Collection<String> newSecondIDs, Collection<String> services, NormalRouterNode node) {
        List<String> selectors = new ArrayList<>();
        Collection<String> cahce = new HashSet<>();
        if (services != null) {
            cahce.addAll(services);
            services.forEach(x -> {
                String first = node.getSecondFirstShip().get(x);
                if (first == null) {
                    return;
                }
                Collection<String> tmp = node.getFirstSeconds().get(first);
                if (tmp == null) {
                    return;
                }
                cahce.addAll(tmp);
            });
        }
        // 1.过滤掉已经使用的service
        if (cahce != null && !cahce.isEmpty()) {
            selectors = newSecondIDs.stream()
                                    .filter(x -> !cahce.contains(x))
                                    .collect(toList());
        } else {
            selectors.addAll(newSecondIDs);
        }

        // 2.判断集合是否为空，为空，则解析失败。
        if (selectors.isEmpty()) {
            throw new IllegalArgumentException("none second server is used");
        }

        // 3.对select 服务进行排序。
        Collections.sort(selectors, new Comparator<String>() {
            @Override
            public int compare(String o1, String o2) {
                return o1.compareTo(o2);
            }
        });

        return selectors;
    }

    /**
     * 计算权值
     *
     * @param services
     *
     * @return
     */
    private static int calcWeight(Collection<String> services, Map<String, Integer> newSecondIDs) {
        // 1.若services的为空则该权值计算无效返回-1
        if (services == null || services.isEmpty()) {
            return -1;
        }

        // 2. 累计权值
        int weight = 0;
        int tmp = -1;
        for (String service : services) {
            if (newSecondIDs.get(service) != null) {
                tmp = newSecondIDs.get(service);
                weight += tmp;
            }
        }

        // 3.若tmp为-1 则表明未匹配上service，则其权值计算无效，范围-1
        return tmp == -1 ? -1 : weight;
    }

    /**
     * 文件名生成数值 V1版本使用的。V2兼容
     *
     * @param fileCode
     * @param size
     *
     * @return
     */
    private static int hashFileName(int fileCode, int size) {
        return fileCode % size;
    }

    /**
     * 路由规则V2版本检索二级serverId逻辑
     *
     * @param chosenServices
     * @param weightValue
     *
     * @return
     */
    private static int searchIndex(List<String> chosenServices, Map<String, Integer> newSecondIDs, int weightValue) {
        if (weightValue == 0) {
            return 0;
        }

        int sum = 0;
        int lastVaild = -1;
        String server = null;
        for (int index = 0; index < chosenServices.size(); index++) {
            server = chosenServices.get(index);
            if (newSecondIDs.get(server) == null) {
                continue;
            }
            sum += newSecondIDs.get(server);
            lastVaild = index;
            if (weightValue <= sum) {
                break;
            }
        }

        return lastVaild;
    }
}
