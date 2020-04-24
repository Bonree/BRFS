package com.bonree.brfs.schedulers.task.manager;

import com.bonree.brfs.schedulers.exception.ParamsErrorException;
import com.bonree.brfs.schedulers.task.meta.SumbitTaskInterface;
import java.util.Properties;

/*****************************************************************************
 * 版权信息：北京博睿宏远数据科技股份有限公司
 * Copyright: Copyright (c) 2007北京博睿宏远数据科技股份有限公司,Inc.All Rights Reserved.
 *
 * @date 2018年3月15日 下午5:44:36
 * @Author: <a href=mailto:zhucg@bonree.com>朱成岗</a>
 * @Description: quartz调度服务
 *****************************************************************************
 */
public interface BaseSchedulerInterface {
    /**
     * 概述：初始化服务配置
     *
     * @param props
     *
     * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
     */
    public void initProperties(Properties props);

    /**
     * 概述：加载任务到服务
     *
     * @param jobConfig
     *
     * @return
     *
     * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
     */
    public boolean addTask(SumbitTaskInterface task) throws ParamsErrorException;

    /**
     * 概述：启动服务
     *
     * @return
     *
     * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
     */
    public void start() throws RuntimeException, NullPointerException;

    /**
     * 概述：重启服务
     *
     * @return
     *
     * @throws
     * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
     */
    public void reStart() throws RuntimeException, NullPointerException;

    /**
     * 概述：关闭服务
     *
     * @return
     *
     * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
     */
    public void close(boolean isWaitTaskComplete) throws Exception;

    /**
     * 概述：判断服务是否已经启动
     *
     * @return
     *
     * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
     */
    public boolean isStart();

    /**
     * 概述：判断服务是否已经销毁
     *
     * @return
     *
     * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
     */
    public boolean isDestory();

    /**
     * 概述：杀死指定的job
     *
     * @param jobConfig
     *
     * @return
     *
     * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
     */
    public boolean deleteTask(SumbitTaskInterface task) throws ParamsErrorException;

    /**
     * 概述：暂停任务
     *
     * @param task
     *
     * @return
     *
     * @throws Exception
     * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
     */
    public boolean pauseTask(SumbitTaskInterface task) throws ParamsErrorException;

    /**
     * 概述：暂停所有任务
     *
     * @return
     *
     * @throws Exception
     * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
     */
    public boolean pauseAllTask();

    /**
     * 概述：重启任务
     *
     * @param task
     *
     * @return
     *
     * @throws Exception
     * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
     */
    public boolean resumeTask(SumbitTaskInterface task) throws ParamsErrorException;

    /**
     * 概述：重启所有任务
     *
     * @return
     *
     * @throws Exception
     * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
     */
    public boolean resumeAllTask();

    /**
     * 概述：获取线程池名称
     *
     * @return
     *
     * @throws Exception
     * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
     */
    public String getInstanceName();

    /**
     * 概述：检查任务
     *
     * @param task
     *
     * @return
     *
     * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
     */
    public void checkTask(SumbitTaskInterface task) throws ParamsErrorException;

    /**
     * 概述：获取任务状态
     *
     * @param task
     *
     * @return
     *
     * @throws SchedulerException
     * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
     */
    public int getTaskStat(SumbitTaskInterface task) throws ParamsErrorException;

    /**
     * 概述：判断任务是否执行
     *
     * @param task
     *
     * @return
     *
     * @throws SchedulerException
     * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
     */
    public boolean isExecuting(SumbitTaskInterface task) throws ParamsErrorException;

    /**
     * 概述：线程池是否已经暂停
     *
     * @return
     *
     * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
     */
    public boolean isPaused();

    /**
     * 概述：暂停线程池
     *
     * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
     */
    public void pausePool();

    /**
     * 概述：恢复线程池
     *
     * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
     */
    public void resumePool();

    /**
     * 概述：获取任务线程池数
     *
     * @return
     *
     * @throws Exception
     * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
     */
    public int getPoolSize();

    /**
     * 概述：获取提交任务数
     *
     * @return
     *
     * @throws Exception
     * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
     */
    public int getSumbitTaskCount();
}
