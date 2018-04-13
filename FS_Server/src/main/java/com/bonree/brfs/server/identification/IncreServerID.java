package com.bonree.brfs.server.identification;

import com.bonree.brfs.common.zookeeper.curator.CuratorClient;

public interface IncreServerID<T> {
    T increServerID(CuratorClient client, String dataNode);
}
