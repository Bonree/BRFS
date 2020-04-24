package com.bonree.brfs.resourceschedule.utils;

import com.bonree.brfs.common.utils.BrStringUtils;
import java.io.File;
import java.util.Collection;

/*****************************************************************************
 * 版权信息：北京博睿宏远数据科技股份有限公司
 * Copyright: Copyright (c) 2007北京博睿宏远数据科技股份有限公司,Inc.All Rights Reserved.
 *
 * @date 2018年3月16日 下午1:38:10
 * @Author: <a href=mailto:zhucg@bonree.com>朱成岗</a>
 * @Description:磁盘工具类
 *****************************************************************************
 */
public class DiskUtils {
    /**
     * 概述：过滤非法的挂载点
     *
     * @param mountPoint 文件分区挂载点
     *
     * @return
     *
     * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
     */
    public static Boolean filterMountPoint(String mountPoint) {
        // 1.挂载点为空返回NULL
        if (BrStringUtils.isEmpty(mountPoint)) {
            return true;
        }
        File mountFile = new File(mountPoint);
        // 2.目录不存在返回NULL
        if (!mountFile.exists()) {
            return true;
        }
        // 3.挂载点为文件返回NULL
        if (mountFile.isFile()) {
            return true;
        }
        return false;
    }

    /**
     * 概述：判断目录属于哪个分区
     *
     * @param dir
     * @param patition
     *
     * @return
     *
     * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
     */
    public static String selectPartOfDisk(String dirpath, Collection<String> mounts) {
        if (BrStringUtils.isEmpty(dirpath)) {
            return null;
        }
        if (mounts == null) {
            return null;
        }
        File dirFile = new File(dirpath);
        if (!dirFile.exists()) {
            return null;
        } else if (mounts.contains(dirFile.getAbsolutePath())) {
            return dirFile.getAbsolutePath();
        } else if (dirFile.getParentFile() != null) {
            return selectPartOfDisk(dirFile.getParentFile().getAbsolutePath(), mounts);
        } else {
            return null;
        }

    }
}
