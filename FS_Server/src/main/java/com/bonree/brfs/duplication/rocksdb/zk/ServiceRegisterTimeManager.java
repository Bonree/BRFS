package com.bonree.brfs.duplication.rocksdb.zk;

import com.bonree.brfs.common.service.Service;
import com.bonree.brfs.common.service.ServiceManager;
import com.bonree.brfs.configuration.Configs;
import com.bonree.brfs.configuration.units.CommonConfigs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/*******************************************************************************
 * 版权信息：北京博睿宏远数据科技股份有限公司
 * Copyright: Copyright (c) 2007博睿宏远科技发展有限公司,Inc.All Rights Reserved.
 *
 * @Date 2020/3/19 17:55
 * @Author: <a href=mailto:zhangqi@bonree.com>张奇</a>
 * @Description:
 ******************************************************************************/
public class ServiceRegisterTimeManager {
    private static final Logger LOG = LoggerFactory.getLogger(ServiceRegisterTimeManager.class);

    private ServiceManager serviceManager;

    public ServiceRegisterTimeManager(ServiceManager serviceManager) {
        this.serviceManager = serviceManager;
    }

    /**
     * @description: 获取注册时间最早的服务
     */
    public Service getEarliestRegisterService() {
        List<Service> services = this.serviceManager.getServiceListByGroup(Configs.getConfiguration().GetConfig(CommonConfigs.CONFIG_REGION_SERVICE_GROUP_NAME));

        if (services.size() != 0) {
            Service tmpService = services.remove(0);
            long tmpTime = tmpService.getRegisterTime();

            for (Service service : services) {
                if (service.getRegisterTime() < tmpTime) {
                    tmpService = service;
                }
            }
            return tmpService;
        }
        return null;

    }

}
