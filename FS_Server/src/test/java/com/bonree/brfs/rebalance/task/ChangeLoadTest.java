package com.bonree.brfs.rebalance.task;

import com.bonree.brfs.common.rebalance.Constants;
import com.bonree.brfs.common.utils.JsonUtils;
import com.bonree.brfs.common.zookeeper.curator.CuratorClient;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import org.apache.curator.utils.ZKPaths;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ChangeLoadTest {
    private static final Logger LOG = LoggerFactory.getLogger(ChangeLoadTest.class);

    @Test
    public void loadChangeTest() throws Exception {
        String zkAddress = "192.168.150.106:2181,192.168.150.107:2181";
        CuratorClient client = CuratorClient.getClientInstance(zkAddress);
        Map<Integer, List<DiskPartitionChangeSummary>> map = new HashMap<>();
        String path = "/brfs/brfs_gxtest/rebalance/changes";
        loadCache(map, client, path);
    }

    public void loadCache(
        Map<Integer, List<DiskPartitionChangeSummary>> changeSummaryCache,
        CuratorClient curatorClient,
        String changesPath) throws Exception {
        List<String> snPaths = curatorClient.getChildren(changesPath); // 此处获得子节点名称
        if (snPaths != null) {
            for (String snNode : snPaths) {
                String snPath = ZKPaths.makePath(changesPath, snNode);
                List<String> childPaths = curatorClient.getChildren(snPath);

                List<DiskPartitionChangeSummary> changeSummaries = new CopyOnWriteArrayList<>();
                if (childPaths != null) {
                    for (String childNode : childPaths) {
                        String childPath = ZKPaths.makePath(snPath, childNode);
                        byte[] data = curatorClient.getData(childPath);
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
}
