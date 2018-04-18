package com.bonree.brfs.server.identification;

import java.util.List;

public interface VirtualServerID {
    

    /** 概述：获取需要的虚拟ServerID
     * @param count
     * @return
     * @user <a href=mailto:weizheng@bonree.com>魏征</a>
     */
    public List<String> getVirtualID(int storageIndex, int count);

    /** 概述：列出使用的virtualID
     * @return
     * @user <a href=mailto:weizheng@bonree.com>魏征</a>
     */
    public List<String> listNormalVirtualID(int storageIndex);

    /** 概述：列出无效的VirtualID
     * @param storageIndex
     * @return
     * @user <a href=mailto:weizheng@bonree.com>魏征</a>
     */
    public List<String> listInvalidVirtualID(int storageIndex);

    /** 概述：列出所有的virtual server ID
     * @param storageIndex
     * @return
     * @user <a href=mailto:weizheng@bonree.com>魏征</a>
     */
    public List<String> listAllVirtualID(int storageIndex);

    /** 概述：无效化虚拟ID
     * @param id
     * @return
     * @user <a href=mailto:weizheng@bonree.com>魏征</a>
     */
    public boolean invalidVirtualIden(int storageIndex, String id);

    /** 概述：删除虚拟ID
     * @param id
     * @return
     * @user <a href=mailto:weizheng@bonree.com>魏征</a>
     */
    public boolean deleteVirtualIden(int storageIndex, String id);
}
