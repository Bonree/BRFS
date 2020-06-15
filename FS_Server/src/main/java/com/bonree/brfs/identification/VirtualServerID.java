package com.bonree.brfs.identification;

import java.util.List;

public interface VirtualServerID {

    /**
     * 概述：获取virtual serverID
     *
     * @param storageIndex sn索引
     * @param count        获取virtual server id 个数
     * @param diskFirstIDs 使用1级serverid，会进行注册，表明自身使用过该虚拟serverid
     *
     * @return
     *
     * @user <a href=mailto:weizheng@bonree.com>魏征</a>
     */
    List<String> getVirtualID(int storageIndex, int count, List<String> diskFirstIDs);

    /**
     * 概述：列出使用的virtualID
     *
     * @return
     *
     * @user <a href=mailto:weizheng@bonree.com>魏征</a>
     */
    List<String> listValidVirtualIds(int storageIndex);

    /**
     * 概述：列出使用的virtualID
     *
     * @return
     *
     * @user <a href=mailto:weizheng@bonree.com>魏征</a>
     */
    List<String> listVirtualIds(int storageIndex);

    /**
     * 判断一级serverid是否存在有效的虚拟serverid数据
     *
     * @param storageIndex
     * @param virtualId
     * @param first
     *
     * @return
     */
    boolean hasVirtual(int storageIndex, String virtualId, String first);

    /**
     * 概述：列出无效的VirtualID
     *
     * @param storageIndex
     *
     * @return
     *
     * @user <a href=mailto:weizheng@bonree.com>魏征</a>
     */
    List<String> listInvalidVirtualIds(int storageIndex);

    /**
     * 概述：无效化虚拟ID
     *
     * @param id
     *
     * @return
     *
     * @user <a href=mailto:weizheng@bonree.com>魏征</a>
     */
    boolean invalidVirtualId(int storageIndex, String id);

    /**
     * 概述：
     *
     * @param storageIndex
     * @param id
     *
     * @return
     *
     * @user <a href=mailto:weizheng@bonree.com>魏征</a>
     */
    boolean validVirtualId(int storageIndex, String id);

    /**
     * 获取包含指定虚拟serverid的一级serverid集合
     *
     * @param storageIndex
     * @param virtual
     *
     * @return
     */
    List<String> listFirstServer(int storageIndex, String virtual);

    /**
     * 概述：删除虚拟ID
     *
     * @param id
     *
     * @return
     *
     * @user <a href=mailto:weizheng@bonree.com>魏征</a>
     */
    boolean deleteVirtualId(int storageIndex, String id);

    /**
     * 概述：获取virtual servers path
     *
     * @return
     *
     * @user <a href=mailto:weizheng@bonree.com>魏征</a>
     */
    String getVirtualIdContainerPath();

    /**
     * 概述：为虚拟ID注册一个1级ID，标识该机器参与过virtual恢复,下次不能选择该server
     *
     * @param storageIndex
     *
     * @return
     *
     * @user <a href=mailto:weizheng@bonree.com>魏征</a>
     */
    void addFirstId(int storageIndex, String virtualID, String firstId);
}
