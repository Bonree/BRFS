package com.bonree.brfs.duplication.datastream.blockcache;

import com.bonree.brfs.common.net.http.HandleResultCallback;
import com.bonree.brfs.common.net.http.data.FSPacket;

public interface WriteRequest {
    HandleResultCallback getHandleResultCallback();

    boolean ifRequestIsTimeOut();

    FSPacket getFsPacket();

    String getSrName();
}
