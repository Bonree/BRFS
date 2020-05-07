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
     * 概述：暂停线程池中的任务
     *
     * @param taskpoolkey 对应的任务
     * @param task        任务信息
     *
     * @return
     *
     * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
     */
    boolean pauseTask(T1 taskpoolkey, T3 task) throws ParamsErrorException;

    /**
     * 概述：恢复线程池中的任务
     *
     * @param task 任务信息
     *
     * @return
     *
     * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
     */
    boolean resumeTask(T1 taskpoolKey, T3 task) throws ParamsErrorException;

    /**
     * 概述：删除线程池中的任务
     *
     * @param task 任务信息
     *
     * @return
     *
     * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
     */
    boolean deleteTask(T1 taskpoolKey, T3 task) throws ParamsErrorException;

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
     * 概述：关闭指定的线程池
     *
     * @param taskpoolKey
     * @param isWaitTaskCompleted
     *
     * @return
     *
     * @throws ParamsErrorException
     * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
     */
    boolean closeTaskPool(T1 taskpoolKey, boolean isWaitTaskCompleted) throws ParamsErrorException;

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
     * 概述：重启任务线程池
     *
     * @param taskpoolKey
     *
     * @return
     *
     * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
     */
    boolean reStartTaskPool(T1 taskpoolKey) throws ParamsErrorException;

    /**
     * 概述：暂停任务线程池，线程池将不在添加新任务，已添加的任务继续保持原有状态
     *
     * @param taskpoolKey 对应的任务
     *
     * @return
     *
     * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
     */
    boolean pauseTaskPool(T1 taskpoolKey) throws ParamsErrorException;

    /**
     * 概述：暂停线程池的所有的任务
     *
     * @param taskpoolKey
     *
     * @return
     *
     * @throws ParamsErrorException
     * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
     */
    boolean pauseAllTask(T1 taskpoolKey) throws ParamsErrorException;

    /**
     * 概述：恢复线程池所有的任务
     *
     * @param taskpoolKey
     *
     * @return
     *
     * @throws ParamsErrorException
     * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
     */
    boolean resumeAllTask(T1 taskpoolKey) throws ParamsErrorException;

    /**
     * 概述：恢复暂停的线程池
     *
     * @param taskpoolKey
     *
     * @return
     *
     * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
     */
    boolean resumeTaskPool(T1 taskpoolKey) throws ParamsErrorException;

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

    /**
     * 概述：获取已经开始的线程池key的集合
     *
     * @return
     *
     * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
     */
    Collection<String> getStartedPoolKeys();

    /**
     * 概述：获取已经关闭的线程池key的集合
     *
     * @return
     *
     * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
     */
    Collection<String> getClosedPoolKeys();

    /**
     * 概述：获取已经暂停的线程池
     *
     * @return
     *
     * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
     */
    Collection<String> getPausePoolKeys();

    /**
     * 概述：指定线程池是否启动
     *
     * @param taskpoolKey
     *
     * @return
     *
     * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
     */
    boolean isStarted(T1 taskpoolKey) throws ParamsErrorException;

    /**
     * 概述：指定线程池是否关闭
     *
     * @param taskpoolKey
     *
     * @return
     *
     * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
     */
    boolean isClosed(T1 taskpoolKey) throws ParamsErrorException;

    /**
     * 概述：指定线程池是否暂停
     *
     * @param taskpoolKey
     *
     * @return
     *
     * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
     */
    boolean isPaused(T1 taskpoolKey) throws ParamsErrorException;

    /**
     * 概述：查看任务状态 任务的状态 -2：不存在对应的线程池，-1：不存在，0：正常，1：暂停，2：完成，3：错误，4：阻塞--正在执行
     *
     * @param taskpoolKey
     * @param task
     *
     * @return
     *
     * @throws ParamsErrorException
     * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
     */
    int getTaskStat(T1 taskpoolKey, T3 task) throws ParamsErrorException;

}
