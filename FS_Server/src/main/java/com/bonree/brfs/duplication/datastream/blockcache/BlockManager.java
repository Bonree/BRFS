package com.bonree.brfs.duplication.datastream.blockcache;

import com.bonree.brfs.common.net.http.HandleResultCallback;
import com.bonree.brfs.common.net.http.data.FSPacket;

/**
 * @author wangchao
 * @date 2020/4/3 - 10:44 上午
 */
public interface BlockManager {

    Block appendToBlock(String srName, FSPacket packet, HandleResultCallback callback);

    void addToWaitingPool(String srName, FSPacket packet, HandleResultCallback callback);

    long getBlockSize();
}
