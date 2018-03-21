package com.bonree.brfs.server.identification;

import java.util.List;

/*******************************************************************************
 * 版权信息：博睿宏远科技发展有限公司
 * Copyright: Copyright (c) 2007博睿宏远科技发展有限公司,Inc.All Rights Reserved.
 * 
 * @date 2018年3月19日 上午11:26:24
 * @Author: <a href=mailto:weizheng@bonree.com>魏征</a>
 * @Description: 服务标识接口
 ******************************************************************************/
public interface Identification {

    public final static int SINGLE = 1;
    public final static int MULTI = 2;
    public final static int VIRTUAL = 3;

    public String getSingleIdentification();

    public String getMultiIndentification();

    public String getVirtureIdentification();
    
    public List<String> loadVirtualIdentification();
}
