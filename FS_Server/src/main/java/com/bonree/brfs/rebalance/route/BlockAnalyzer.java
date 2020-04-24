package com.bonree.brfs.rebalance.route;

/*******************************************************************************
 * 版权信息： 北京博睿宏远数据科技股份有限公司
 * Copyright (c) 2007-2020 北京博睿宏远数据科技股份有限公司，Inc. All Rights Reserved.
 * @date 2020年03月30日 17:21:45
 * @author: <a href=mailto:zhucg@bonree.com>朱成岗</a>
 * @description: 文件块解析接口，将文件块名称解析为对应的逻辑
 ******************************************************************************/

public interface BlockAnalyzer {
    /**
     * 根据文件块解析可用的服务
     *
     * @param fileName
     *
     * @return
     */
    String[] searchVaildIds(String fileName);

    void update();
}
