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
public interface ServerIDOpt {

    public final static int FIRST_ID = 1;
    public final static int SECOND_ID = 2;
    public final static int VIRTUAL_ID = 3;

    /** 概述：生成单副本ServerID
     * @return
     * @user <a href=mailto:weizheng@bonree.com>魏征</a>
     */
    public String genFirstIdentification();

    /** 概述：生成多副本ServerID
     * @return
     * @user <a href=mailto:weizheng@bonree.com>魏征</a>
     */
    public String genSecondIndentification();

    /** 概述：生成虚拟ServerID
     * @return
     * @user <a href=mailto:weizheng@bonree.com>魏征</a>
     */
    public String genVirtualIdentification(int storageIndex);

    /** 概述：获取需要的虚拟ServerID
     * @param count
     * @return
     * @user <a href=mailto:weizheng@bonree.com>魏征</a>
     */
    public List<String> getVirtualIdentification(int storageIndex, int count);

    /** 概述：列出使用的virtualID
     * @return
     * @user <a href=mailto:weizheng@bonree.com>魏征</a>
     */
    public List<String> listVirtualIdentification(int storageIndex);

    /** 概述：无效化虚拟ID
     * @param id
     * @return
     * @user <a href=mailto:weizheng@bonree.com>魏征</a>
     */
    public boolean invalidVirtualIden(int storageIndex, String id);

    /** 概述：删除虚拟ID
     * @param id
     * @return
     * @user <a href=mailto:weizheng@bonree.com>魏征</a>
     */
    public boolean deleteVirtualIden(int storageIndex, String id);
}
