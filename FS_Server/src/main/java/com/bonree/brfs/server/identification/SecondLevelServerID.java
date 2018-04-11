package com.bonree.brfs.server.identification;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/*******************************************************************************
 * 版权信息：博睿宏远科技发展有限公司
 * Copyright: Copyright (c) 2007博睿宏远科技发展有限公司,Inc.All Rights Reserved.
 * 
 * @date 2018年4月11日 下午3:31:55
 * @Author: <a href=mailto:weizheng@bonree.com>魏征</a>
 * @Description: 二级serverID用于有副本的SN来使用，各个SN的平衡情况会不一致，
 * 所以每个SN都会有自己的二级ServerID。
 ******************************************************************************/
public class SecondLevelServerID {
    private ServerIDGen serverIdGen;
    String multiFile;
    private Map<Integer, String> multiMap;

    public SecondLevelServerID(String multiFile, ServerIDGen serverIdGen) {
        this.multiFile = multiFile;
        this.serverIdGen = serverIdGen;
        multiMap = new ConcurrentHashMap<>();
    }

    public Map<Integer,String> getServerId() {
        return multiMap;
    }

}
