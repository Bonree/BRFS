package com.bonree.brfs.server.identification;

public interface VirtualServerIDGen {
    
    public final static int VIRTUAL_ID = 3;

    /** 概述：生成虚拟ServerID
     * @return
     * @user <a href=mailto:weizheng@bonree.com>魏征</a>
     */
    public String genVirtualID(int storageIndex);

}
