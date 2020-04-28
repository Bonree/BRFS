package com.bonree.brfs.duplication.filenode.duplicates;

import com.bonree.brfs.common.utils.Pair;
import com.bonree.brfs.resourceschedule.model.LimitServerResource;
import com.bonree.brfs.resourceschedule.model.ResourceModel;
import java.util.List;

/*****************************************************************************
 * 版权信息：北京博睿宏远数据科技股份有限公司
 * Copyright: Copyright (c) 2007北京博睿宏远数据科技股份有限公司,Inc.All Rights Reserved.
 *
 * @date 2018年3月16日 上午10:49:46
 * @Author: <a href=mailto:zhucg@bonree.com>朱成岗</a>
 * @Description:可用server接口
 *****************************************************************************
 */
public interface ResourceSelector {

    /**
     * 概述：获取可用server集合
     * 过期 下一版本删除
     *
     * @param scene               场景枚举
     * @param exceptionServerList 异常server集合
     *
     * @return
     *
     * @throws Exception
     * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
     */
    @Deprecated
    List<Pair<String, Integer>> selectAvailableServers(int scene, String storageName, List<String> exceptionServerList,
                                                       int centSize) throws Exception;

    /**
     * 概述：获取可用server集合
     * 过期 下一版本删除
     *
     * @param scene               场景枚举
     * @param exceptionServerList 异常server集合
     *
     * @return
     *
     * @throws Exception
     * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
     */
    @Deprecated
    List<Pair<String, Integer>> selectAvailableServers(int scene, int snId, List<String> exceptionServerList, int centSize)
        throws Exception;

    /**
     * 概述：设置异常过滤指标
     *
     * @param limits
     *
     * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
     */
    void setLimitParameter(LimitServerResource limits);

    /**
     * 概述：更新资源数据
     * 过期 下一版本删除
     *
     * @param resource key： serverId, resourceModel
     *
     * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
     */
    @Deprecated
    void update(ResourceModel resource);

    /***
     * 概述：添加资源
     * 过期 下一版本删除
     * @param resources
     * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
     */
    @Deprecated
    void add(ResourceModel resources);

    /**
     * 概述：移除资源
     * 过期 下一版本删除
     *
     * @param resource
     *
     * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
     */
    @Deprecated
    void remove(ResourceModel resource);
}
