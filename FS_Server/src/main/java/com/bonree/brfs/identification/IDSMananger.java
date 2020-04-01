package com.bonree.brfs.identification;

import com.bonree.brfs.identification.impl.FirstLevelServerIDImpl;
import com.bonree.brfs.identification.impl.SecondIDRelationShip;

/*******************************************************************************
 * 版权信息： 北京博睿宏远数据科技股份有限公司
 * Copyright (c) 2007-2020 北京博睿宏远数据科技股份有限公司，Inc. All Rights Reserved.
 * @date 2020年03月31日 15:22:55
 * @author: <a href=mailto:zhucg@bonree.com>朱成岗</a>
 * @description: ID管理器
 ******************************************************************************/

public class IDSMananger {
    /**
     * 一级serverid
     */
    private String firstServerId;
    /**
     * 一级serverid生成器
     */
    private FirstLevelServerIDImpl firstLevelServerID = null;
    /**
     * 二级serverid生成器
     */
    private SecondIDRelationShip secondIDRelationShip;
}
