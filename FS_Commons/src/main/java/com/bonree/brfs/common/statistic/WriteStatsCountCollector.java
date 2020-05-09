package com.bonree.brfs.common.statistic;

import com.google.common.collect.Maps;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class WriteStatsCountCollector {
    private static Map<String, WriteCountModel> statsMap = Maps.newConcurrentMap();
    ExecutorService executor = Executors.newFixedThreadPool(20);

    public void submit(String srName, long startTime) {
        executor.submit(() -> {
            record(srName, startTime);
        });
    }

    public void cacheRecord(String srName, long startTime) {
        synchronized (statsMap) {
            if (statsMap.containsKey(srName)) {
                WriteCountModel writeCountModel = statsMap.get(srName);
                writeCountModel.addCountByTime(startTime);
            } else {
                WriteCountModel writeCountModel = new WriteCountModel();
                writeCountModel.addCountByTime(startTime);
                statsMap.put(srName, writeCountModel);
            }
        }
    }

    void record(String srName, long startTime) {
        cacheRecord(srName, startTime);
    }

    public void printMonitorInfo() {
        // 统计汇总信息
        if (!statsMap.isEmpty()) {
            for (WriteCountModel statsModel : statsMap.values()) {
                statsModel.print();
            }
        }

    }


    private Map<String, WriteCountModel> popAll(Map<String, WriteCountModel> map) {
        Map<String, WriteCountModel> tmpMap = Maps.newHashMap();
        synchronized (map) {
            tmpMap.putAll(map);
            map.clear();
        }
        return tmpMap;
    }
}
