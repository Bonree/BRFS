package com.bonree.brfs.resource.utils;

import com.bonree.brfs.common.utils.BrStringUtils;
import java.io.File;

/*****************************************************************************
 * 版权信息：北京博睿宏远数据科技股份有限公司
 * Copyright: Copyright (c) 2007北京博睿宏远数据科技股份有限公司,Inc.All Rights Reserved.
 *
 * @date 2018年3月15日 上午9:54:34
 * @Author: <a href=mailto:zhucg@bonree.com>朱成岗</a>
 * @Description: 第三方lib加载工具类
 *****************************************************************************
 */
public class LibUtils {
    /**
     * 概述：sigar配置path，在使用sigar前必须引用第三方依赖包，否则程序报错
     *
     * @throws Exception
     * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
     */
    public static void loadLibraryPath(String libPath) throws Exception {
        if (BrStringUtils.isEmpty(libPath)) {
            throw new NullPointerException("[config error] sigar lib path is empty !!!");
        }
        File file = new File(libPath);
        if (!file.exists()) {
            throw new NullPointerException("[config error] sigar lib path is not exists !!! path : " + libPath);
        }
        String path = System.getProperty("java.library.path");
        if (OSCheckUtils.getOperatingSystemType() == OSCheckUtils.OSType.Windows) {
            path += ";" + libPath;
        } else {
            path += ":" + libPath;
        }
        System.setProperty("java.library.path", path);
    }
}
