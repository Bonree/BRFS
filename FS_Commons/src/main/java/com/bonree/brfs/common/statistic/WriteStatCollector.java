package com.bonree.brfs.common.statistic;

import com.google.common.collect.Maps;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import javax.inject.Inject;

public class WriteStatCollector {
    private static Map<String, WriteCountModel> statsMap = Maps.newConcurrentMap();
    ExecutorService executor = Executors.newFixedThreadPool(20);

    @Inject
    public WriteStatCollector() {
    }

    public void submit(String srName) {
        executor.submit(() -> {
            record(srName);
        });
    }

    public void cacheRecord(String srName) {
        synchronized (statsMap) {
            if (statsMap.containsKey(srName)) {
                WriteCountModel writeCountModel = statsMap.get(srName);
                writeCountModel.addWriteCount(1);
            } else {
                WriteCountModel writeCountModel = new WriteCountModel(1);
                statsMap.put(srName, writeCountModel);
            }
        }
    }

    public void addCount(String srName, long count) {
        synchronized (statsMap) {
            if (statsMap.containsKey(srName)) {
                WriteCountModel writeCountModel = statsMap.get(srName);
                writeCountModel.addWriteCount(count);
            } else {
                WriteCountModel writeCountModel = new WriteCountModel(count);
                statsMap.put(srName, writeCountModel);
            }
        }
    }

    void record(String srName) {
        cacheRecord(srName);
    }

    public void printMonitorInfo() {
        // 统计汇总信息
        if (!statsMap.isEmpty()) {
            for (WriteCountModel statsModel : statsMap.values()) {
                statsModel.print();
            }
        }

    }

    public Map<String, WriteCountModel> popAll() {
        Map<String, WriteCountModel> tmpMap = Maps.newHashMap();
        synchronized (statsMap) {
            tmpMap.putAll(statsMap);
            statsMap.clear();
        }
        return tmpMap;
    }
}
