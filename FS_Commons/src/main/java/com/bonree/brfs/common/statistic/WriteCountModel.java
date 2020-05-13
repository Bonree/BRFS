package com.bonree.brfs.common.statistic;

import com.google.common.collect.Maps;
import java.util.Map;

public class WriteCountModel {
    private Map<Long, Integer> countByTime = Maps.newConcurrentMap();

    public int getCountByTime(long startTime) {
        return countByTime.get(startTime);
    }

    public void addCountByTime(long startTime) {
        synchronized (countByTime) {
            if (countByTime.containsKey(startTime)) {
                countByTime.put(startTime, countByTime.get(startTime) + 1);
            } else {
                countByTime.put(startTime, 1);
            }
        }
    }

    public void print() {
        if (!countByTime.isEmpty()) {
            for (long startTime : countByTime.keySet()) {
                System.out.println(startTime + ":" + countByTime.get(startTime));
            }
        }
    }
}
