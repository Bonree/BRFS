package com.bonree.brfs.server.identification;

import java.util.List;

/*******************************************************************************
 * 版权信息：博睿宏远科技发展有限公司
 * Copyright: Copyright (c) 2007博睿宏远科技发展有限公司,Inc.All Rights Reserved.
 * 
 * @date 2018年4月12日 上午11:37:08
 * @Author: <a href=mailto:weizheng@bonree.com>魏征</a>
 * @Description: 获取virtual server ID，virtual server ID在迁移时，也是根据SN来
 * 进行迁移的，所以每个SN的迁移进度不一样。故需要分开维护
 ******************************************************************************/
public class VirtualServerID {

    // TODO 这个虚拟Server ID目前没有用缓存。可能会影响效率。可以考虑使用cache进行监控
    private ServerIDOpt serverIdOpt;

    public VirtualServerID(ServerIDOpt serverIdOpt) {
        this.serverIdOpt = serverIdOpt;
    }

    public List<String> getServerId(int storageIndex, int count) {
        List<String> virtualServerIds = serverIdOpt.getVirtualIdentification(storageIndex, count);
        return virtualServerIds;
    }

    public boolean invalidVirtualID(int storageIndex, String id) {
        return serverIdOpt.invalidVirtualIden(storageIndex, id);
    }

    public boolean deleteVirtualID(int storageIndex, String id) {
        return serverIdOpt.deleteVirtualIden(storageIndex, id);
    }

}
