package com.bonree.brfs.rebalance.task;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.Test;

public class TaskDispatcherTest {
    @Test
    public void testIsSameFirst() {
        Map<String, String> map = ImmutableMap.of("26","12","27","10","28","10","29","11");
        List<String> seconds = ImmutableList.of("27", "28");
        System.out.println(!isSameFirst(map, seconds));
    }

    private boolean isSameFirst(Map<String, String> secondFirstMap, List<String> seconds) {
        Map<String, Integer> countMap = new HashMap<>();
        seconds.stream().forEach(second -> {
            String first = secondFirstMap.get(second);
            if (countMap.get(first) == null) {
                countMap.put(first, 1);
            } else {
                countMap.put(first, countMap.get(first) + 1);
            }
        });
        return countMap.size() <= 1;
    }
}
