package com.bonree.brfs.rebalance.recover;

import java.util.ArrayList;
import java.util.List;

import com.bonree.brfs.rebalance.DataRecover;
import com.bonree.brfs.server.model.ServerInfoModel;

/*******************************************************************************
 * 版权信息：博睿宏远科技发展有限公司
 * Copyright: Copyright (c) 2007博睿宏远科技发展有限公司,Inc.All Rights Reserved.
 * 
 * @date 2018年3月23日 下午2:16:13
 * @Author: <a href=mailto:weizheng@bonree.com>魏征</a>
 * @Description: 恢复虚拟ServerID的
 ******************************************************************************/
public class VirtualRecover implements DataRecover {

    public VirtualRecover() {
    }

    @Override
    public void recover() {
        ServerInfoModel localServer = new ServerInfoModel();
        ServerInfoModel remoteServer = new ServerInfoModel();
        List<String> files = getFiles();
        for (String fileName : files) {
            if (isExistFile(remoteServer, fileName)) {
                remoteCopyFile(localServer, remoteServer, fileName);
            }
        }
    }

    public List<String> getFiles() {
        return new ArrayList<String>();
    }

    public boolean isExistFile(ServerInfoModel remoteServer, String fileName) {
        return true;
    }

    public void remoteCopyFile(ServerInfoModel sourceServer, ServerInfoModel remoteServer, String fileName) {

    }

}
