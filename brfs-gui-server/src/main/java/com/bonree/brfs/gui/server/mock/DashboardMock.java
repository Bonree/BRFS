/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.bonree.brfs.gui.server.mock;

import com.bonree.brfs.gui.server.TimedData;
import com.bonree.brfs.gui.server.TotalDiskUsage;
import com.bonree.brfs.gui.server.node.NodeState;
import com.bonree.brfs.gui.server.node.NodeSummaryInfo;
import com.bonree.brfs.gui.server.stats.BusinessStats;
import com.bonree.brfs.gui.server.stats.DataStatistic;
import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.weakref.jmx.internal.guava.collect.Lists;

public class DashboardMock {

    private static final List<String> bs = ImmutableList.of(
        "Server",
        "net",
        "SDK"
    );

    private static final List<String> nodes = ImmutableList.of(
        "brfs_node_01",
        "brfs_node_02",
        "brfs_node_03",
        "brfs_node_04",
        "brfs_node_05",
        "brfs_node_06",
        "brfs_node_07",
        "brfs_node_08",
        "brfs_node_09"
    );

    private static final List<TimedData<DataStatistic>> allStats = new LinkedList<>();
    private static final Map<String, List<TimedData<DataStatistic>>> stats = new HashMap<>();

    static {
        int maxCount = 60 * 24;
        ScheduledExecutorService exe = Executors.newSingleThreadScheduledExecutor(new ThreadFactory() {

            @Override
            public Thread newThread(Runnable r) {
                Thread t = new Thread(r);
                t.setDaemon(true);
                return t;
            }
        });

        exe.scheduleWithFixedDelay(() -> {
            Calendar now = Calendar.getInstance();
            now.clear(Calendar.SECOND);
            now.clear(Calendar.MILLISECOND);

            final AtomicLong totalWrite = new AtomicLong();
            final AtomicLong totalRead = new AtomicLong();
            for (String b : bs) {
                stats.compute(b, (key, list) -> {
                    if (list == null) {
                        list = new LinkedList<>();
                    }

                    int write = ThreadLocalRandom.current().nextInt(3000, 40000);
                    int read = ThreadLocalRandom.current().nextInt(5000, 60000);
                    totalWrite.addAndGet(write);
                    totalRead.addAndGet(read);
                    list.add(new TimedData<DataStatistic>(
                        now.getTimeInMillis(),
                        new DataStatistic(write, read)));
                    int removed = list.size() - maxCount;
                    for (int i = 0; i < removed; i++) {
                        list.remove(0);
                    }

                    return list;
                });
            }

            allStats.add(new TimedData<DataStatistic>(
                now.getTimeInMillis(),
                new DataStatistic(totalWrite.get(), totalRead.get())));

            int removed = allStats.size() - maxCount;
            for (int i = 0; i < removed; i++) {
                allStats.remove(0);
            }

        }, 0, 1, TimeUnit.MINUTES);
    }

    public List<NodeSummaryInfo> getNodeSummaries() {
        return Lists.transform(nodes, DashboardMock::buildNodeSummaryInfo);
    }

    public TotalDiskUsage getTotalDiskUsage() {
        return new TotalDiskUsage(
            ThreadLocalRandom.current().nextLong(5000L, 50000000000L),
            ThreadLocalRandom.current().nextLong(1000L, 10000000000L));
    }

    public List<String> getBusinesses() {
        return bs;
    }

    public BusinessStats getBusinessStats(String business, int minutes) {
        if (!stats.containsKey(business)) {
            return new BusinessStats(business, ImmutableList.of());
        }

        List<TimedData<DataStatistic>> datas = stats.get(business);
        int index = Math.max(0, datas.size() - minutes);
        ArrayList<TimedData<DataStatistic>> result = new ArrayList<>();
        for (int i = index; i < datas.size(); i++) {
            result.add(datas.get(i));
        }

        return new BusinessStats(business, result);
    }

    public BusinessStats getAllBusinessStats(int minutes) {
        int index = Math.max(0, allStats.size() - minutes);
        ArrayList<TimedData<DataStatistic>> result = new ArrayList<>();
        for (int i = index; i < allStats.size(); i++) {
            result.add(allStats.get(i));
        }

        return new BusinessStats("", result);
    }

    private static NodeSummaryInfo buildNodeSummaryInfo(String node) {
        double cpuUsage = nextDouble();
        double memUsage = nextDouble();
        double systemDiskUsage = nextDouble();
        double brfsDiskUsage = nextDouble();

        boolean alert = cpuUsage > 0.95 || memUsage > 0.9 || systemDiskUsage > 0.80 || brfsDiskUsage > 0.85;
        NodeState state = ThreadLocalRandom.current().nextInt(100) < 10
            ? NodeState.OFFLINE : (alert ? NodeState.ALERT : NodeState.ONLINE);
        return new NodeSummaryInfo(
            state,
            node,
            "192.168.4." + node.charAt(node.length() - 1),
            cpuUsage,
            memUsage,
            brfsDiskUsage,
            systemDiskUsage);
    }

    private static double nextDouble() {
        return ThreadLocalRandom.current().nextInt(100, 10000) / 10000.0d;
    }
}
