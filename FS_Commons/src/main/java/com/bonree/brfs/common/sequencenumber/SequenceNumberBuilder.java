package com.bonree.brfs.common.sequencenumber;

public interface SequenceNumberBuilder {
    /**
     * 利用zk的自增临时节点获得一个全局唯一的整数
     * @return
     * @throws Exception
     */
    int nextSequenceNumber() throws Exception;
}
