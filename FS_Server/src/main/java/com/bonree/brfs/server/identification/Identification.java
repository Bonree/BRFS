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

    /** 概述：生成单副本ServerID
     * @return
     * @user <a href=mailto:weizheng@bonree.com>魏征</a>
     */
    public String genSingleIdentification();

    /** 概述：生成多副本ServerID
     * @return
     * @user <a href=mailto:weizheng@bonree.com>魏征</a>
     */
    public String genMultiIndentification();

    /** 概述：生成虚拟ServerID
     * @return
     * @user <a href=mailto:weizheng@bonree.com>魏征</a>
     */
    public String genVirtureIdentification();

    /** 概述：获取需要的虚拟ServerID
     * @param count
     * @return
     * @user <a href=mailto:weizheng@bonree.com>魏征</a>
     */
    public List<String> getVirtualIdentification(int count);
    
    /** 概述：列出使用的virtualID
     * @return
     * @user <a href=mailto:weizheng@bonree.com>魏征</a>
     */
    public List<String> listVirtualIdentification();
    
    
    /** 概述：无效化虚拟ID
     * @param id
     * @return
     * @user <a href=mailto:weizheng@bonree.com>魏征</a>
     */
    public boolean invalidVirtualIden(String id);
    
    
    /** 概述：删除虚拟ID
     * @param id
     * @return
     * @user <a href=mailto:weizheng@bonree.com>魏征</a>
     */
    public boolean deleteVirtualIden(String id);
}
