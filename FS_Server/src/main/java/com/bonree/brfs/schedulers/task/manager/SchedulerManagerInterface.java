package com.bonree.brfs.schedulers.task.manager;

import com.bonree.brfs.common.process.LifeCycle;
import com.bonree.brfs.schedulers.exception.ParamsErrorException;
import java.util.Collection;
import java.util.Properties;

/******************************************************************************
 * 版权信息：北京博睿宏远数据科技股份有限公司
 * Copyright: Copyright (c) 2007北京博睿宏远数据科技股份有限公司,Inc.All Rights Reserved.
 *
 * @date 2018年3月27日 下午3:44:57
 * @Author: <a href=mailto:zhucg@bonree.com>朱成岗</a>
 * @Description: 任务服务接口
 *****************************************************************************
 */
public interface SchedulerManagerInterface<T1, T2, T3> extends LifeCycle {
    /**
     * 概述：将任务添加到指定的线程池
     *
     * @param taskpoolkey 对应的任务
     * @param task        任务信息
     *
     * @return
     *
     * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
     */
    boolean addTask(T1 taskpoolkey, T3 task) throws ParamsErrorException;


    /**
     * 概述：销毁任务线程池，相应的key将被移除
     *
     * @param isWaitTaskCompleted 等待任务完成
     *
     * @return
     *
     * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
     */
    boolean destoryTaskPool(T1 taskpoolKey, boolean isWaitTaskCompleted) throws ParamsErrorException;


    /**
     * 概述：创建任务线程池
     *
     * @param taskpoolKey 对应的任务
     * @param prop        线程池配置
     *
     * @return
     *
     * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
     */
    boolean createTaskPool(T1 taskpoolKey, Properties prop) throws ParamsErrorException;

    /**
     * 概述：启动任务线程池
     *
     * @param taskpoolKey
     *
     * @return
     *
     * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
     */
    boolean startTaskPool(T1 taskpoolKey) throws ParamsErrorException;


    /**
     * 概述：获取已提交的任务数
     *
     * @param taskpoolKey
     *
     * @return
     *
     * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
     */
    int getSumbitedTaskCount(T1 taskpoolKey) throws ParamsErrorException;

    /**
     * 概述：获取任务线程池大小
     *
     * @param taskpoolKey
     *
     * @return
     *
     * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
     */
    int getTaskPoolSize(T1 taskpoolKey) throws ParamsErrorException;

    /**
     * 概述：获取所有线程池的key
     *
     * @return
     *
     * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
     */
    Collection<String> getAllPoolKey();

}
