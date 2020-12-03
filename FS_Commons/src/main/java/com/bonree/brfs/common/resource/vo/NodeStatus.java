package com.bonree.brfs.common.resource.vo;

/**
 * todo 这个类的位置需要调整么?
 * datanode 节点状态
 */
public enum NodeStatus {
    /**
     * zk上对应的dn 元数据节点信息为空
     * 此时该dn还未注册到该集群中
     */
    EMPTY,
    NORMAL,
    /**
     * 得到一个zk的id后,从empty状态变更为这个状态
     */
    ONLY_SERVER,
    ONLY_PARTITION
}
