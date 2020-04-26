package com.bonree.brfs.partition;

import com.bonree.brfs.identification.LocalPartitionInterface;
import com.bonree.brfs.partition.model.LocalPartitionInfo;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/*******************************************************************************
 * 版权信息： 北京博睿宏远数据科技股份有限公司
 * Copyright (c) 2007-2020 北京博睿宏远数据科技股份有限公司，Inc. All Rights Reserved.
 * @date 2020年03月25日 20:16:28
 * @author: <a href=mailto:zhucg@bonree.com>朱成岗</a>
 * @description:
 ******************************************************************************/

public class LocalPartitionCache implements LocalPartitionListener, LocalPartitionInterface {
    Map<String, LocalPartitionInfo> idToLocal = new ConcurrentHashMap<>();
    Map<String, String> pathToId = new ConcurrentHashMap<>();

    public LocalPartitionCache(Collection<LocalPartitionInfo> idToLocal) {
        for (LocalPartitionInfo part : idToLocal) {
            add(part);
        }
    }

    @Override
    public void remove(LocalPartitionInfo partitionInfo) {
        if (idToLocal.containsKey(partitionInfo.getPartitionId())) {
            idToLocal.remove(partitionInfo.getPartitionId());
        }
        if (pathToId.containsKey(partitionInfo.getDataDir())) {
            pathToId.remove(partitionInfo.getDataDir());
        }
    }

    @Override
    public void add(LocalPartitionInfo partitionInfo) {
        idToLocal.put(partitionInfo.getPartitionId(), partitionInfo);
        pathToId.put(partitionInfo.getDataDir(), partitionInfo.getPartitionId());
    }

    @Override
    public String getDataPaths(String partitionId) {
        return idToLocal.get(partitionId) == null ? null : idToLocal.get(partitionId).getDataDir();
    }

    @Override
    public String getPartitionId(String dataPath) {
        return pathToId.get(dataPath);
    }

    @Override
    public Collection<String> listPartitionId() {
        return idToLocal.keySet();
    }

    @Override
    public Collection<LocalPartitionInfo> getPartitions() {
        return idToLocal.values();
    }
}
