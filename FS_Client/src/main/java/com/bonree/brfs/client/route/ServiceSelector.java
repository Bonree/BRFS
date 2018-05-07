package com.bonree.brfs.client.route;

import com.bonree.brfs.common.service.Service;

public interface ServiceSelector {

    /** 概述：获取硬盘空间较好的服务来写数据
     * @return
     * @user <a href=mailto:weizheng@bonree.com>魏征</a>
     */
    public Service selectWriteService();

    /** 概述：选择文件所在的服务来读取
     * @param partFid 格式uuid_sid1_sid2
     * @return
     * @user <a href=mailto:weizheng@bonree.com>魏征</a>
     */
    public Service selectReadService(String partFid);

    /** 概述：随机选择一个服务
     * @return
     * @user <a href=mailto:weizheng@bonree.com>魏征</a>
     */
    public Service selectRandomService();

}
