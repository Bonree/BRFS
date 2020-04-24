package com.bonree.brfs.common.zookeeper.curator.cache;

import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;

/*******************************************************************************
 * 版权信息：博睿宏远科技发展有限公司
 * Copyright: Copyright (c) 2007博睿宏远科技发展有限公司,Inc.All Rights Reserved.
 *
 * @date 2018年3月16日 下午3:50:34
 * @Author: <a href=mailto:weizheng@bonree.com>魏征</a>
 * @Description: 主要为了对listen进行命名
 ******************************************************************************/
public abstract class AbstractPathChildrenCacheListener implements PathChildrenCacheListener {
    private final static String DEFAULT_NAME = "default";

    private String listenName;

    public AbstractPathChildrenCacheListener(String listenName) {
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
