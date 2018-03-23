package com.bonree.brfs.common.zookeeper.curator.cache;

import org.apache.curator.framework.recipes.cache.TreeCacheListener;

/*******************************************************************************
 * 版权信息：博睿宏远科技发展有限公司
 * Copyright: Copyright (c) 2007博睿宏远科技发展有限公司,Inc.All Rights Reserved.
 * 
 * @date 2018年3月22日 下午4:32:06
 * @Author: <a href=mailto:weizheng@bonree.com>魏征</a>
 * @Description: 
 ******************************************************************************/
public abstract class AbstractTreeCacheListener implements TreeCacheListener {
    private final static String DEFAULT_NAME = "default";

    private String listenName;

    public AbstractTreeCacheListener(String listenName) {
        if (listenName == null) {
            this.listenName = DEFAULT_NAME;
        } else {
            this.listenName = listenName;
        }
    }

    public String getListenName() {
        return listenName;
    }

}
