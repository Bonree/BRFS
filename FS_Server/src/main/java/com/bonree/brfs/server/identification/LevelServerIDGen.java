package com.bonree.brfs.server.identification;


/*******************************************************************************
 * 版权信息：博睿宏远科技发展有限公司
 * Copyright: Copyright (c) 2007博睿宏远科技发展有限公司,Inc.All Rights Reserved.
 * 
 * @date 2018年3月19日 上午11:26:24
 * @Author: <a href=mailto:weizheng@bonree.com>魏征</a>
 * @Description: 服务标识接口
 ******************************************************************************/
public interface LevelServerIDGen {

    public final static int FIRST_ID = 1;
    public final static int SECOND_ID = 2;

    /** 概述：生成级别ServerID
     * @return
     * @user <a href=mailto:weizheng@bonree.com>魏征</a>
     */
    public String genLevelID();
    
}
