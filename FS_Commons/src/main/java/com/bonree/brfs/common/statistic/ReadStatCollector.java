package com.bonree.brfs.common.statistic;

import com.google.common.collect.Maps;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import javax.inject.Inject;

public class ReadStatCollector {
    private static Map<String, ReadCountModel> statsMap = Maps.newConcurrentMap();
    ExecutorService executor = Executors.newFixedThreadPool(20);

    @Inject
    public ReadStatCollector() {
    }

    public void submit(String srName) {
        executor.submit(() -> {
            record(srName);
        });
    }

    public void cacheRecord(String srName) {
        synchronized (statsMap) {
            if (statsMap.containsKey(srName)) {
                ReadCountModel readCountModel = statsMap.get(srName);
                readCountModel.addReadCount();
            } else {
                ReadCountModel readCountModel = new ReadCountModel(0, srName);
                readCountModel.addReadCount();
                statsMap.put(srName, readCountModel);
            }
        }
    }

    void record(String srName) {
        cacheRecord(srName);
    }

    public void printMonitorInfo() {
        // 统计汇总信息
        if (!statsMap.isEmpty()) {
            for (ReadCountModel statsModel : statsMap.values()) {
                statsModel.print();
            }
        }

    }


    public Map<String, ReadCountModel> popAll() {
        Map<String, ReadCountModel> tmpMap = Maps.newHashMap();
        synchronized (statsMap) {
            tmpMap.putAll(statsMap);
            statsMap.clear();
        }
        return tmpMap;
    }

}
