package com.bonree.brfs.resourceschedule.utils;

/*****************************************************************************
 * 版权信息：北京博睿宏远数据科技股份有限公司
 * Copyright: Copyright (c) 2007北京博睿宏远数据科技股份有限公司,Inc.All Rights Reserved.
 *
 * @date 2018年3月16日 下午1:38:47
 * @Author: <a href=mailto:zhucg@bonree.com>朱成岗</a>
 * @Description:操作系统类型工具
 *****************************************************************************
 */
public final class OSCheckUtils {
    public enum OSType {
        Windows, MacOS, Linux, Other
    }

    protected static OSType detectedOS;

    public static OSType getOperatingSystemType() {
        if (detectedOS == null) {
            String os = System.getProperty("os.name", "generic").toLowerCase();
            if (os.indexOf("win") >= 0) {
                detectedOS = OSType.Windows;
            } else if ((os.indexOf("mac") >= 0) || (os.indexOf("darwin") >= 0)) {
                detectedOS = OSType.MacOS;
            } else if (os.indexOf("nux") >= 0) {
                detectedOS = OSType.Linux;
            } else {
                detectedOS = OSType.Other;
            }
        }
        return detectedOS;
    }
}

