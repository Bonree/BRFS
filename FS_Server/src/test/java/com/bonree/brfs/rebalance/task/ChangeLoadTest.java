package com.bonree.brfs.rebalance.task;

import com.bonree.brfs.common.utils.JsonUtils;
import com.bonree.brfs.common.utils.TimeUtils;
import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import org.apache.commons.io.FileUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryNTimes;
import org.apache.curator.utils.ZKPaths;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ChangeLoadTest {
    private static final Logger LOG = LoggerFactory.getLogger(ChangeLoadTest.class);

    @Test
    public void loadChangeTest() throws Exception {
        String zkAddress = "192.168.150.106:2181,192.168.150.107:2181";
        CuratorFramework client = CuratorFrameworkFactory.newClient(zkAddress, new RetryNTimes(10, 1000));
        client.start();
        client.blockUntilConnected();
        Map<Integer, List<DiskPartitionChangeSummary>> map = new HashMap<>();
        String path = "/brfs/brfs_gxtest/rebalance/changes";
        loadCache(map, client, path);
    }

    public void loadCache(
        Map<Integer, List<DiskPartitionChangeSummary>> changeSummaryCache,
        CuratorFramework curatorClient,
        String changesPath) throws Exception {
        List<String> snPaths = curatorClient.getChildren().forPath(changesPath); // 此处获得子节点名称
        if (snPaths != null) {
            for (String snNode : snPaths) {
                String snPath = ZKPaths.makePath(changesPath, snNode);
                List<String> childPaths = curatorClient.getChildren().forPath(snPath);

                List<DiskPartitionChangeSummary> changeSummaries = new CopyOnWriteArrayList<>();
                if (childPaths != null) {
                    for (String childNode : childPaths) {
                        String childPath = ZKPaths.makePath(snPath, childNode);
                        byte[] data = curatorClient.getData().forPath(childPath);
                        LOG.info("current data:{}", new String(data));
                        DiskPartitionChangeSummary cs = JsonUtils.toObjectQuietly(data, DiskPartitionChangeSummary.class);
                        changeSummaries.add(cs);
                    }
                }
                // 如果该目录下有服务变更信息，则进行服务变更信息保存
                if (!changeSummaries.isEmpty()) {
                    // 需要对changeSummary进行已时间来排序
                    Collections.sort(changeSummaries);
                    changeSummaryCache.put(Integer.parseInt(snNode), changeSummaries);
                }
            }
        }
    }

    @Test
    public void loadChanges() throws Exception {
        String path = this.getClass().getResource("/changesArray.json").getPath();
        File changesFile = new File(path);
        byte[] data = FileUtils.readFileToByteArray(changesFile);
        DiskPartitionChangeSummary[] array = JsonUtils.toObjectQuietly(data, DiskPartitionChangeSummary[].class);
        List<DiskPartitionChangeSummary> list = new ArrayList<>();
        for (DiskPartitionChangeSummary summary : array) {
            list.add(summary);
        }
        Collections.sort(list);
        list.stream().forEach(ele -> {
            long time = Long.parseLong(ele.getChangeID().substring(0,10))*1000;
            String timeStr = TimeUtils.formatTimeStamp(time,TimeUtils.TIME_MILES_FORMATE);
            System.out.println(timeStr + ":" + ele.getChangePartitionId() + "->" + ele.getChangeType());
        });
    }
}
