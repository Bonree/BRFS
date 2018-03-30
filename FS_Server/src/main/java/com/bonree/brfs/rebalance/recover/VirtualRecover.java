package com.bonree.brfs.rebalance.recover;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bonree.brfs.rebalance.DataRecover;
import com.bonree.brfs.rebalance.task.BalanceTaskSummary;
import com.bonree.brfs.server.ServerInfo;
import com.bonree.brfs.server.StorageName;

/*******************************************************************************
 * 版权信息：博睿宏远科技发展有限公司
 * Copyright: Copyright (c) 2007博睿宏远科技发展有限公司,Inc.All Rights Reserved.
 * 
 * @date 2018年3月23日 下午2:16:13
 * @Author: <a href=mailto:weizheng@bonree.com>魏征</a>
 * @Description: 恢复虚拟ServerID的
 ******************************************************************************/
public class VirtualRecover implements DataRecover {

    private final static Logger LOG = LoggerFactory.getLogger(VirtualRecover.class);

    private static final String NAME_SEPARATOR = "_";

    private StorageName storageName;

    private BalanceTaskSummary balanceSummary;

    public VirtualRecover(BalanceTaskSummary balanceSummary) {
        this.balanceSummary = balanceSummary;
    }

    @Override
    public void recover() {
        List<String> files = getFiles();
        String remoteServerId = balanceSummary.getInputServers().get(0);
        String fixServerId = balanceSummary.getServerId();
        LOG.info("balance virtual serverId:" + fixServerId);
        for (String fileName : files) {
            int replicaPot = 0;
            String[] metaArr = fileName.split(NAME_SEPARATOR);
            List<String> fileServerIds = new ArrayList<>();
            for (int j = 1; j < metaArr.length; j++) {
                fileServerIds.add(metaArr[j]);
            }

            if (fileServerIds.contains(fixServerId)) {
                replicaPot = fileServerIds.indexOf(fixServerId);
                if (!isExistFile(remoteServerId, fileName)) {
                    remoteCopyFile(remoteServerId, fileName, replicaPot);
                }
            }
        }
    }

    public List<String> getFiles() {
        return new ArrayList<String>();
    }

    public boolean isExistFile(String remoteServerId, String fileName) {
        return true;
    }

    public void remoteCopyFile(String remoteServerId, String fileName, int replicaPot) {

    }

}
