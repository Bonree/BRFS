package com.bonree.brfs.client.route;

import java.util.List;

import com.bonree.brfs.common.service.Service;

public interface ServiceSelector {
    
    /** 概述：选择一个Service
     * @param params
     * @return
     * @user <a href=mailto:weizheng@bonree.com>魏征</a>
     */
    public Service selectService();
    
    public List<Service> selectServiceList();
}
