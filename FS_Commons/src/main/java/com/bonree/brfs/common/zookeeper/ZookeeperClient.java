package com.bonree.brfs.common.zookeeper;

import java.util.List;

import org.apache.curator.framework.api.CuratorListener;
import org.apache.zookeeper.Watcher;

/*******************************************************************************
 * 版权信息：博睿宏远科技发展有限公司
 * Copyright: Copyright (c) 2007博睿宏远科技发展有限公司,Inc.All Rights Reserved.
 * 
 * @date 2018年3月9日 下午2:39:44
 * @Author: <a href=mailto:weizheng@bonree.com>魏征</a>
 * @Description: zookeeper client接口，用于进行zookeeper操作
 ******************************************************************************/
public interface ZookeeperClient {

    void createPersistent(String path, boolean isRecursion);

    void createPersistent(String path, boolean isRecursion, byte[] data);

    void createPersistentSequential(String path, boolean isRecursion);

    void createPersistentSequential(String path, boolean isRecursion, byte[] data);

    void createEphemeral(String path, boolean isRecursion);

    void createEphemeral(String path, boolean isRecursion, byte[] data);

    void createEphemeralSequential(String path, boolean isRecursion);

    void createEphemeralSequential(String path, boolean isRecursion, byte[] data);

    void setData(String path, byte[] data);
    
    byte[] getData(String path);

    void setDataAsync(String path, byte[] data, CuratorListener listener);

    void delete(String path, boolean isRecursion);

    void guaranteedDelete(String path, boolean isRecursion);

    boolean checkExists(String path);

    List<String> getChildren(String path);

    List<String> watchedGetChildren(String path);

    List<String> watchedGetChildren(String path, Watcher watcher);

    boolean isConnected();

    void close();
}
