package com.bonree.brfs.client.route;

import com.bonree.brfs.common.service.Service;

public interface ServiceMetaInfo {
    Service getFirstServer();
    int getReplicatPot();
}
